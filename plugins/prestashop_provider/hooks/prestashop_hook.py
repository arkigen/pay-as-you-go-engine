from airflow.hooks.base import BaseHook
from prestapyt import PrestaShopWebServiceDict
import requests

PS_REQUEST_TIMEOUT = (5, 30)


def _build_prestashop_session(api_key):
    session = requests.Session()
    session.trust_env = False
    session.auth = (api_key, '')
    original_request = session.request

    def request_with_timeout(*args, **kwargs):
        kwargs.setdefault('timeout', PS_REQUEST_TIMEOUT)
        return original_request(*args, **kwargs)

    session.request = request_with_timeout
    return session

class PrestaShopHook(BaseHook):
    conn_name_attr = 'prestashop_conn_id'
    default_conn_name = 'prestashop_default'
    conn_type = 'prestashop'
    hook_name = 'PrestaShop'

    def __init__(self, prestashop_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.prestashop_conn_id = prestashop_conn_id
        self.client = None

    def get_conn(self) -> PrestaShopWebServiceDict:
        if self.client:
            return self.client

        conn = self.get_connection(self.prestashop_conn_id)
        
        # Connection parameters can be passed via Airflow Connection
        # Host: URL
        # Password: API Key
        
        url = conn.host
        api_key = conn.password
        
        self.client = PrestaShopWebServiceDict(url, api_key, session=_build_prestashop_session(api_key))
        return self.client

    def test_connection(self):
        try:
            client = self.get_conn()
            client.get('')
            return True, "Connection successful"
        except Exception as e:
            return False, str(e)
