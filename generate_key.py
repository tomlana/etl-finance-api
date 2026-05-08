import secrets

def generate_api_key():
    return secrets.token_hex(32)

if __name__ == "__main__":
    print(generate_api_key())
