from dotenv import load_dotenv
load_dotenv()

from openai import OpenAI

client = OpenAI()

user_message = "I want to book a table for 4 people tonight at 7pm"

response = client.responses.create(
    model="gpt-4o-mini",
    input=user_message
)

print("\nAI RESPONSE:")
print(response.output_text)

