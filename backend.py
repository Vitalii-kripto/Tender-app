import uvicorn

if __name__ == "__main__":
    print("Starting TenderSmart Backend System (OOP Structure)...")
    # Runs the app defined in backend/main.py
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000, reload=True)
