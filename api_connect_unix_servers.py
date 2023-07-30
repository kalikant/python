import paramiko
from fastapi import FastAPI, HTTPException

app = FastAPI()

def run_unix_command_on_server(server_address, username, password, command):
    try:
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(server_address, username=username, password=password)
        stdin, stdout, stderr = ssh_client.exec_command(command)
        output = stdout.read().decode().strip()
        ssh_client.close()
        return output
    except paramiko.AuthenticationException as e:
        raise HTTPException(status_code=401, detail="Authentication failed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/run_command/")
async def run_command_on_multiple_servers(command: str, server_info: list):
    results = {}
    for server in server_info:
        server_address = server.get("address")
        username = server.get("username")
        password = server.get("password")
        if not server_address or not username or not password:
            raise HTTPException(status_code=400, detail="Incomplete server information.")
        
        result = run_unix_command_on_server(server_address, username, password, command)
        results[server_address] = result

    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
