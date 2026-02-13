from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "Demo Backend Placeholder"}

@app.get("/{path:path}")
def catch_all(path: str):
    return {"status": "Demo Mode Active"}
