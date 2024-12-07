import openai

API_KEY = "rayon_8d2MhJjOA1NILI27P5I9cks7EfwURHeu"
client = openai.OpenAI(api_key=API_KEY, base_url="https://api.nineteen.ai/v1")

# API_KEY = "rayon_lUDlX800y37Lpoyx08pfIcKpEFgxVPD3"  
# client = openai.OpenAI(api_key=API_KEY, base_url="http://localhost:8000/v1")


# With stream!

print("\nStarting stream test...")
print("-" * 100)
stream = client.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": "Say this is a test for a streaming response, in 10 ish words, nothing else",
        }
    ],
    model="chat-llama-3-1-70b",
    stream=True,
)
for chunk in stream:
    print(chunk.choices[0].delta.content or "", end="")

print("\n" + "-" * 100)
print("\nStream test complete, now for non-stream...")

print("-" * 100)

# Non-stream

response = client.chat.completions.create(
    messages=[
        {
            "role": "user",
            "content": "Say this is a test for non-streaming response, in 10 ish words, nothing else",
        }
    ],
    model="chat-llama-3-1-70b",
)

print(response.choices[0].message.content)

print("-" * 100)
print("Non-stream test complete!")
