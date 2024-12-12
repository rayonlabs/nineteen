# Guide to use SN19 with Cursor

### **Open settings**
Follow these steps on a mac:

![Setting up Cursor on Mac](cursor_settings.png)

If you're on ubuntu or windows, please visit https://www.apple.com/shop/buy-mac for further instructions


### **Add a new model, named exactly `hugging-quants/Meta-Llama-3.1-70B-Instruct-AWQ-INT4`**

It's very important the name matches exactly


### **Configure custom 'OPENAI' endpoint**

First, disable all other OpenAI models. Don't worry, they're degraded to hell anyway


Use:

Override OpenAI Base URL: `https://api.nineteen.ai/v1` 

Key: Visit https://nineteen.ai/app/api and sign up for an API key, add the same to `OpenAI API Key` field

![OPENAI Key Configuration](openai_key.png)



# Done
Enjoy :)