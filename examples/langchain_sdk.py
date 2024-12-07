from langchain_core.messages import HumanMessage
from langchain_openai import ChatOpenAI

API_KEY = "YOUR_API_KEY_HERE"
chat = ChatOpenAI(
    api_key=API_KEY,
    base_url="https://api.nineteen.ai/v1",
    model="chat-llama-3-1-70b",
)

print("\nStarting stream test..." + "\n" + "-" * 100)

stream_messages = [
    HumanMessage(content="Say this is a test for a streaming response, in 10 ish words, nothing else")
]

for chunk in chat.stream(stream_messages):
    print(chunk.content, end="")

print("\n" + "-" * 100 + "\n\n" + "Stream test complete, now for non-stream..." + "\n" + "-" * 100)


messages = [
    HumanMessage(content="Say this is a test for non-streaming response, in 10 ish words, nothing else")
]

response = chat.invoke(messages)
print(response.content)

print("-" * 100 + "\n" + "Non-stream test complete!")
