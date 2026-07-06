from airflow.plugins_manager import AirflowPlugin
from prestashop_provider.hooks.prestashop_hook import PrestaShopHook
from prestashop_provider.hooks.odoo_hook import OdooHook

class PrestaShopOdooPlugin(AirflowPlugin):
    name = "prestashop_odoo_plugin"
    hooks = [PrestaShopHook, OdooHook]
