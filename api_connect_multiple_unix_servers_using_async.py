import asyncio
import paramiko
from fastapi import FastAPI, HTTPException
from httpx import AsyncClient

app = FastAPI()

async def run_unix_command_on_server(client, server_address, command):
    try:
        stdin, stdout, stderr = await client.exec_command(command)
        output = stdout.read().decode().strip()
        return output
    except paramiko.AuthenticationException as e:
        raise HTTPException(status_code=401, detail="Authentication failed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

async def execute_command_on_servers(command, server_info):
    async with AsyncClient() as http_client:
        tasks = []
        for server in server_info:
            server_address = server.get("address")
            username = server.get("username")
            private_key_path = server.get("private_key_path")
            if not server_address or not username or not private_key_path:
                raise HTTPException(status_code=400, detail="Incomplete server information.")
            
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=server_address, username=username, key_filename=private_key_path)

            tasks.append(run_unix_command_on_server(ssh_client, server_address, command))

        results = await asyncio.gather(*tasks)
        
        for ssh_client in tasks:
            ssh_client.close()

        return results

@app.post("/run_command/")
async def run_command_on_multiple_servers(command: str, server_info: list):
    results = await execute_command_on_servers(command, server_info)
    return {server_info[i]["address"]: result for i, result in enumerate(results)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
