import os
from dotenv import load_dotenv
load_dotenv()

SECURITY_PROTOCOL="SASL_SSL"
SSL_MECHANISM="PLAIN"
API_KEY=os.getenv("API_KEY",None)
BOOTSTRAP_SERVER=os.getenv("BOOTSTRAP_SERVER",None)
API_SECRET_KEY=os.getenv("API_SECRET_KEY",None)
ENDPOINT_SCHEMA_URL=os.getenv("ENDPOINT_SCHEMA_URL",None)
SCHEMA_REGISTER_API_KEY=os.getenv("SCHEMA_REGISTER_API_KEY",None)
SCHEMA_REGISTER_SECRET_KEY=os.getenv("SCHEMA_REGISTER_SECRET_KEY",None)

def sasl_conf():
    sasl_conf_={
        "sasl.mechanism":SSL_MECHANISM,
        "bootstrap.servers":BOOTSTRAP_SERVER,
        "security.protocol":SECURITY_PROTOCOL,
        "sasl.username":API_KEY,
        "sasl.password":API_SECRET_KEY
    }
    return sasl_conf_


def schema_registry_config():
    return {"url":ENDPOINT_SCHEMA_URL,"basic.auth.user.info":f"{SCHEMA_REGISTER_API_KEY}:{SCHEMA_REGISTER_SECRET_KEY}"}

