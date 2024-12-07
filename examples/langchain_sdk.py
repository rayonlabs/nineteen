from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

API_KEY = "rayon_8d2MhJjOA1NILI27P5I9cks7EfwURHeu"

# Initialize the ChatOpenAI model with nineteen.ai base URL
chat = ChatOpenAI(
    api_key=API_KEY,
    base_url="https://api.nineteen.ai/v1",
    model="chat-llama-3-1-70b",
)

# Streaming example
print("\nStarting stream test...")
print("-" * 100)

stream_messages = [
    HumanMessage(content="Say this is a test for a streaming response, in 10 ish words, nothing else")
]

for chunk in chat.stream(stream_messages):
    print(chunk.content, end="")

print("\n" + "-" * 100)
print("\nStream test complete, now for non-stream...")

print("-" * 100)

# Non-streaming example
messages = [
    HumanMessage(content="Say this is a test for non-streaming response, in 10 ish words, nothing else")
]

response = chat.invoke(messages)
print(response.content)

print("-" * 100)
print("Non-stream test complete!")
