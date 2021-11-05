
import sys
import time
import ray

import pandas as pd
import requests as rq

""" Run this script locally to execute a Ray program on your Ray cluster on
Kubernetes.

Before running this script, you must port-forward from the local host to
the relevant Kubernetes head service e.g.
kubectl -n ray port-forward service/example-cluster-ray-head 10001:10001.

Set the constant LOCAL_PORT below to the local port being forwarded.
"""
LOCAL_PORT = 10001

def log(s):
    print(f"{time.time()} {s}")
    sys.stdout.flush()

@ray.remote
def make_a_request(rq, pd, df, auth, si, ei): 

    with open("/etc/hostname", 'r') as f: 
        podname = f.read().strip()

    results = []

    for row in df.iloc[si:ei,:].iterrows():

        imei, vin, when = row[1].imei, row[1].vin, row[1].when

        rt = {
            "podname": podname, 
            "imei": imei,
            "vin": vin,
            "when": when,
            "err": [],
            "device": None, 
            "vehicle": None, 
        }
        results.append(rt)

        try: 

            url = f"http://devices-telematics.tl-core.svc.cluster.local/v1-alpha/devices"
            params = {
                'query': f"device_imei={imei}"
            }
            rs = rq.get(url, headers=auth, params=params)
            if (rs.status_code == 200) and (len(rs.json()) > 0):
                device_id = rs.json()[0]['oid']
                rt['device'] = device_id
            elif ((rs.status_code == 200) and (len(rs.json()) == 0)) or (rs.status_code == 204):
                data = {
                    'device_type': "pulse",
                    'device_imei': str(imei),
                }
                rs = rq.post(url, headers=auth, json=data)
                if rs.status_code in [200,201]:
                    device_id = rs.json()['oid']
                    rt['device'] = device_id
                else:
                    rt['err'].append(rs.text)
            
            url  = f"http://vehicles-telematics.tl-core.svc.cluster.local/v1-alpha/vehicles"
            params = {
                'query': f"vin={vin}"
            }
            rs = rq.get(url, headers=auth, params=params)
            if (rs.status_code == 200) and (len(rs.json()) > 0):
                vehicle_id = rs.json()[0]['oid']
                rt['vehicle'] = vehicle_id
            elif ((rs.status_code == 200) and (len(rs.json()) == 0)) or (rs.status_code == 204):
                data = {'vin': vin}
                rs = rq.post(url, headers=auth, json=data)
                if rs.status_code in [200,201]:
                    vehicle_id = rs.json()['oid']
                    rt['vehicle'] = vehicle_id
                else:
                    rt['err'].append(rs.text)

            if rt['device'] and rt['vehicle']:
            
                url = f"http://bindings-telematics.tl-core.svc.cluster.local/v1-alpha/bindings"
                params = {
                    'query': f"vehicle_id={vehicle_id},device.device_id={device_id}"
                }
                rs = rq.get(url, headers=auth, params=params)
                if (rs.status_code == 200) and (len(rs.json()) > 0):
                    binding_id = rs.json()[0]['oid']
                    rt['binding_id'] = binding_id
                    if len(rs.json()) > 1:
                        rt['err'].append(f"multiple bindings: {[b['oid'] for b in rs.json()]}")
                elif ((rs.status_code == 200) and (len(rs.json()) == 0)) or (rs.status_code == 204):
                    data = {
                        'vehicle_id': vehicle_id,
                        'device': {'device_id': device_id, "start_date": when},
                        'account': {'account_id': "000000000000000000000000", "start_date": 0}
                    }
                    rs = rq.post(url, headers=auth, json=data)
                    if rs.status_code in [200,201]:
                        binding_id = rs.json()['oid']
                        rt['binding_id'] = binding_id
                    else:
                        rt['err'].append(rs.text)

        except Exception as err: 
            rt['err'].append(f"{err.__class__.__name__} {err}")

    return results


def wait_for_nodes(expected):
    # Wait for all nodes to join the cluster.
    while True:
        resources = ray.cluster_resources()
        node_keys = [key for key in resources if "node" in key]
        num_nodes = sum(resources[node_key] for node_key in node_keys)
        if num_nodes < expected:
            print("{} nodes have joined so far, waiting for {} more.".format(
                num_nodes, expected - num_nodes))
            sys.stdout.flush()
            time.sleep(1)
        else:
            break


def main():

    log("starting up")

    api_host = "https://api.telematics-dev.metromile.com"
    api_version = "v1-alpha"

    email = input("Email to get token for: ")

    if '@' not in email:
        email += "@metromile.com"

    url = f"{api_host}/{api_version}/login"
    data = {'user_email': email}
    rs = rq.post(url, json=data)
    rs.raise_for_status()

    otp_code = input("Application OTP from email: ")

    url = f"{api_host}/{api_version}/token"
    data['otp_code'] = otp_code
    rs = rq.post(url, json=data)
    rs.raise_for_status()

    token = rs.json()['access_token']
    auth = {'Authorization': f"Bearer {token}"}

    # skipping for efficiency in testing
    # auth = {'Authorization': f"Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6IkdUQnhCWjE4a1pwb3EySS0xTm9wOCJ9.eyJodHRwczovL21ldHJvbWlsZS5jb20vdXNlcnMvcHJlZmVycmVkX3VzZXJuYW1lIjoicm1vcnJvd0BtZXRyb21pbGUuY29tIiwiaXNzIjoiaHR0cHM6Ly9kZXYtYWhkLXdhMG4udXMuYXV0aDAuY29tLyIsInN1YiI6ImVtYWlsfDYwYjlhZjVhMzgyNGEyZjM1NzhmYmJlOSIsImF1ZCI6WyJodHRwczovL21tLWRldi1zZXJ2aWNlcy9hcGkiLCJodHRwczovL2Rldi1haGQtd2Ewbi51cy5hdXRoMC5jb20vdXNlcmluZm8iXSwiaWF0IjoxNjMxMzc3MjcwLCJleHAiOjE2MzE0NjM2NzAsImF6cCI6ImpSOTN6eDRkQ282enBkTkpOcHBjY2k5aVByR0tKbndSIiwic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBhZGRyZXNzIHBob25lIG9mZmxpbmVfYWNjZXNzIiwiZ3R5IjoicGFzc3dvcmQiLCJwZXJtaXNzaW9ucyI6WyJhZG1pbjptbXNlcnZpY2VzIiwicmVhZDptbWFjY291bnRzIiwicmVhZDptbXBhY3NlcnZpY2UiLCJyZWFkOm1tc2VydmljZXMiLCJ3cml0ZTptbXBhY3NlcnZpY2UiLCJ3cml0ZTptbXNlcnZpY2VzIl19.md7ybp32bj8gz2yFhvDd_OmAIZfUmNIY6ODwJYjUNp_jwEAmsONLiEQbUy-zv6H6s7cEiOB9QY_A_Z6_8fadKJdAOtPRcGWVN6n7VFpWSP5Stx_M7cvSKgN4IPs996Mi9LpIpyzKjkA5bJ6uxBxjqpUE9Ukw55CsYuubzb-PG-o-dMQKz5PKhzxaj_Ri38VbdAntvc-wtk7jPTOfsUAmoqoJzzIXhzr1YUvTiEbo_Un6Ap6kVJuXpoetBo90GmobmsfeXKDT245Pu8i6uruw377GyFDnj1f3ufxCoWHvbFyUFAgL5w2mfncOmmu5GeGLaFi2-DuHwvfHnh6C1apR2A"}

    # pause for ray cluster readiness (right?)
    log("waiting for nodes...")
    wait_for_nodes(3)

    # read data
    df = pd.read_csv("uniques-2.csv")
    log("read data...")

    # put dependencies
    rqi = ray.put(rq)
    pdi = ray.put(pd)
    dfi = ray.put(df)
    ahi = ray.put(auth)

    W = 100 # number of "tasks" to run
    N = df.shape[0] # how many records to process
    R = N % W # remainder
    B = int((N-R)/W) # base block size
    L = [B + (1 if w < R else 0) for w in range(W)] # always distribute remainder for balance
    G = [sum(L[:w]) for w in range(W+1)] # cumsum (naive way)

    # inititiate the block tasks
    futures = [ 
        make_a_request.remote(rqi, pdi, dfi, ahi, G[w], G[w+1])
        for w in range(W)
    ]
    log("started block tasks...")

    # get the results
    all_results = ray.get(futures)
    results = pd.DataFrame([
        result for rset in all_results for result in rset
    ])
    results.to_csv("load_results.csv", index=False)

    log("finished!")


if __name__ == "__main__":
    ray.init(f"ray://ray.telematics-dev.gcp.metromile.net:{LOCAL_PORT}")
    main()
