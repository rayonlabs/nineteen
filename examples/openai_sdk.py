import openai

API_KEY = "YOUR_API_KEY_HERE"
client = openai.OpenAI(api_key=API_KEY, base_url="https://api.nineteen.ai/v1")


# With stream!

print("\nStarting stream test..." + "\n" + "-" * 100)

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

print("\n" + "-" * 100 + "\n\n" + "Stream test complete, now for non-stream..." + "\n" + "-" * 100)

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
print("-" * 100 + "\n" + "Non-stream test complete!")
