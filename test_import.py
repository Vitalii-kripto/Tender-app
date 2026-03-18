try:
    from backend.main import app
    print("✅ backend.main imported successfully")
except Exception as e:
    print(f"❌ backend.main import failed: {e}")
    import traceback
    traceback.print_exc()
