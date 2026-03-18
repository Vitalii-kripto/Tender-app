import os
from google import genai
from google.genai import types
from dotenv import load_dotenv

load_dotenv()

def test_gemini():
    api_key = os.getenv("GEMINI_API_KEY") or os.getenv("API_KEY")
    if not api_key:
        print("❌ No API KEY found")
        return

    print(f"Using API KEY: {api_key[:10]}...")
    
    try:
        client = genai.Client(api_key=api_key)
        response = client.models.generate_content(
            model='gemini-2.5-flash',
            contents="Hello, are you working?"
        )
        print("✅ Gemini Response:", response.text)
    except Exception as e:
        print("❌ Gemini Error:", e)

if __name__ == "__main__":
    test_gemini()
