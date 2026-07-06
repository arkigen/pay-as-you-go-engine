import xmlrpc.client
from airflow.hooks.base import BaseHook

class OdooHook(BaseHook):
    conn_name_attr = 'odoo_conn_id'
    default_conn_name = 'odoo_default'
    conn_type = 'odoo'
    hook_name = 'Odoo'

    def __init__(self, odoo_conn_id: str = default_conn_name, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.odoo_conn_id = odoo_conn_id
        self.common = None
        self.models = None
        self.uid = None
        self.db = None
        self.password = None

    def get_conn(self):
        if self.models:
            return self.models

        conn = self.get_connection(self.odoo_conn_id)
        url = conn.host
        self.db = conn.schema
        username = conn.login
        self.password = conn.password

        self.common = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/common')
        self.uid = self.common.authenticate(self.db, username, self.password, {})
        self.models = xmlrpc.client.ServerProxy(f'{url}/xmlrpc/2/object')
        
        return self.models

    def execute(self, model, method, *args, **kwargs):
        self.get_conn()
        return self.models.execute_kw(self.db, self.uid, self.password, model, method, args, kwargs)

    def test_connection(self):
        try:
            self.get_conn()
            if self.uid:
                return True, "Connection successful"
            return False, "Authentication failed"
        except Exception as e:
            return False, str(e)
