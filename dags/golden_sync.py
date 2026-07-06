from airflow import DAG
from airflow.decorators import task
from datetime import datetime, timedelta
import json
import os
import sys

# Ensure the core package is in the path
sys.path.append('/Users/codesystemsco/Documents/Innovation/prestashop-odoo/18.0/innovation-connector-core')

from innovation_connector_core.client import InnovationConnectorClient
from innovation_connector_core.consumption import consumption_variable_key, report_sync_result_to_odoo

DAG_DOC_MD = """
### Golden Sync: PrestaShop ↔ Odoo Middleware
This DAG acts as the central orchestrator for the **Pay-As-You-Go** synchronization model.
It handles 11 component types in both directions (Stage 1 import, Stage 2 export).

#### Features:
- **Bidirectional Traceability**: Uses bindings in Odoo (`innovation.prestashop.binding`).
- **Quota Enforcement**: Limits sync based on remaining free/paid requests.
- **Mass Data Handling**: Optimized for large record sets using batch fetching.
"""

with DAG(
    dag_id='golden_sync',
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['innovation', 'prestashop', 'odoo'],
    doc_md=DAG_DOC_MD,
    max_active_runs=5,
    max_active_tasks=5,
    is_paused_upon_creation=False,
    default_args={
        'owner': 'innovation',
        'retries': 0,
        'execution_timeout': timedelta(minutes=5),
    }
) as dag:

    @task(
        execution_timeout=timedelta(minutes=5),
        max_active_tis_per_dag=5,
        doc_md="""
### Sync Task: Process Records
This task performs the actual data movement between PrestaShop and Odoo.
It uses the `innovation-connector-core` library.

**Inputs (via conf):**
- `component_type`: The type of data to sync (e.g., product, category).
- `ps_config`: PrestaShop API credentials and URL.
- `odoo_config`: Odoo XML-RPC credentials and URL.
"""
    )
    def process_sync(**context):
        """
        Main task to process information through WSDL/Webservice.
        Invokes the core package to handle the data flow.
        """
        # Avoid macOS proxy lookup hangs inside Airflow worker subprocesses.
        os.environ.setdefault('NO_PROXY', '*')
        os.environ.setdefault('no_proxy', '*')

        conf = context.get('dag_run').conf or {}
        
        # Connection parameters passed from Odoo
        ps_config = conf.get('ps_config', {})
        odoo_config = conf.get('odoo_config', {})
        component_type = conf.get('component_type', 'product')
        connector_id = conf.get('connector_id', 1)
        incremental = conf.get('incremental', False)
        since = conf.get('since')
        stage = conf.get('stage', 1)
        direction = conf.get('direction', 'prestashop_to_odoo')
        dag_run = context.get('dag_run')
        dag_run_id = getattr(dag_run, 'run_id', None) or context.get('run_id')
        
        if stage not in (1, 2):
            return {"status": "failed", "message": "Only stage 1 and stage 2 are supported."}
        
        # Account Identification (URL Pair)
        ps_url = ps_config.get('url')
        odoo_db = odoo_config.get('db')
        
        # Billing Integration (Centralized)
        billing_url = conf.get('billing_url')
        billing_api_key = conf.get('billing_api_key')
        platform_hash = conf.get('platform_hash')
        
        if not ps_url or not odoo_db:
            print("ERROR: Missing configuration parameters (ps_url or odoo_db).")
            return {"status": "failed", "message": "Missing configuration parameters."}

        account_id = consumption_variable_key(ps_url, odoo_db)
        
        from airflow.models import Variable
        import json
        import requests
        
        # 1. Calculate Remaining Quota (Priority: Billing Server -> Airflow Variable)
        remaining_quota = 100
        use_central_billing = bool(billing_url and billing_api_key and platform_hash)
        
        if use_central_billing:
            try:
                print(f"Checking credits on Billing Server: {billing_url}")
                sync_url = f"{billing_url.rstrip('/')}/innovation-billing/billing/consumption/sync"
                headers = {"API-KEY": billing_api_key}
                payload = {
                    "platform_hash": platform_hash,
                    "consumed_increment": 0,
                }
                resp = requests.post(sync_url, json=payload, headers=headers, timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    remaining_quota = data.get('remaining_credits', 0)
                    print(f"Centralized Billing: {remaining_quota} credits available.")
                else:
                    print(f"WARNING: Billing Server returned {resp.status_code}: {resp.text}")
                    use_central_billing = False # Fallback
            except Exception as e:
                print(f"WARNING: Could not connect to Billing Server: {e}")
                use_central_billing = False # Fallback

        if not use_central_billing:
            try:
                consumption_raw = Variable.get(account_id, default_var='{"consumed": 0, "total": 0}')
                consumption = json.loads(consumption_raw)
                FREE_LIMIT = 100
                current_used = consumption.get('consumed', 0)
                remaining_quota = max(0, FREE_LIMIT - current_used)
            except Exception as e:
                print(f"Error checking legacy consumption: {e}")
                consumption = {"consumed": 0, "total": 0}
                remaining_quota = 100

        if remaining_quota <= 0:
            print(f"BLOCK: Quota exhausted for {platform_hash or account_id}.")
            return {"status": "quota_exhausted", "synced_count": 0, "message": "Quota reached. Please purchase credits."}

        print(f"Starting sync for component: {component_type} (Remaining Quota: {remaining_quota})")
        
        try:
            client = InnovationConnectorClient(odoo_config=odoo_config, ps_config=ps_config)

            # 2. Invoke sync with the remaining quota as the limit
            results = {"status": "failed", "message": "Unknown component"}
            sync_args = {"limit": remaining_quota}
            if incremental:
                sync_args['since'] = since
            
            print(f"Executing {component_type} stage-{stage} sync ({direction}, incremental={incremental})...")
            if stage == 2 or direction == 'odoo_to_prestashop':
                export_handlers = {
                    'category': client.export_categories,
                    'brand': client.export_brands,
                    'supplier': client.export_suppliers,
                    'attribute': client.export_attributes,
                    'discount': client.export_discounts,
                    'product': client.export_products,
                    'product_image': client.export_product_images,
                    'carrier': client.export_carriers,
                    'payment_provider': client.export_payment_providers,
                    'customer': client.export_customers,
                    'address': client.export_addresses,
                    'order': client.export_orders,
                    'invoice': client.export_invoices,
                    'delivery': client.export_deliveries,
                    'credit_slip': client.export_credit_slips,
                    'stock': client.export_stock,
                }
                handler = export_handlers.get(component_type)
                if handler:
                    results = handler(**sync_args)
                else:
                    results = {"status": "failed", "message": f"Unknown export component: {component_type}"}
            elif incremental:
                results = client.sync_incremental(
                    component_type,
                    since=since,
                    limit=remaining_quota,
                )
            else:
                # Map component types to client methods
                import_handlers = {
                    'category': client.sync_categories,
                    'brand': client.sync_brands,
                    'supplier': client.sync_suppliers,
                    'attribute': client.sync_attributes,
                    'discount': client.sync_discounts,
                    'product': client.sync_products,
                    'carrier': client.sync_carriers,
                    'payment_provider': client.sync_payment_providers,
                    'customer': client.sync_customers,
                    'address': client.sync_addresses,
                    'order': client.sync_orders,
                    'invoice': client.sync_invoices,
                    'delivery': client.sync_deliveries,
                    'credit_slip': client.sync_credit_slips,
                    'product_image': client.sync_product_images,
                    'stock': client.sync_stock_from_prestashop,
                }
                handler = import_handlers.get(component_type)
                if handler:
                    results = handler(**sync_args)
                else:
                    results = {"status": "failed", "message": f"Unknown import component: {component_type}"}
                
            # 3. Report consumption (Centralized or Legacy)
            synced_count = results.get('updated_count', results.get('synced_count', 0))
            scanned_count = results.get('scanned_count', synced_count)
            results['component_type'] = component_type
            results['scanned_count'] = scanned_count
            results['updated_count'] = synced_count

            if synced_count > 0:
                if use_central_billing:
                    try:
                        print(f"Reporting {synced_count} consumed credits to Billing Server...")
                        sync_url = f"{billing_url.rstrip('/')}/innovation-billing/billing/consumption/sync"
                        headers = {"API-KEY": billing_api_key}
                        payload = {
                            "platform_hash": platform_hash,
                            "consumed_increment": synced_count,
                        }
                        requests.post(sync_url, json=payload, headers=headers, timeout=15)
                    except Exception as e:
                        print(f"ERROR: Failed to report consumption to Billing Server: {e}")
                else:
                    consumption['consumed'] = consumption.get('consumed', 0) + synced_count
                    consumption['total'] = consumption.get('total', 0) + synced_count
                    Variable.set(account_id, json.dumps(consumption))
                    print(f"Legacy consumption updated for {account_id}: +{synced_count} records.")

            # 4. Return billed/scanned counts to Odoo for Pay-As-You-Go UI
            try:
                odoo_report = report_sync_result_to_odoo(
                    odoo_config,
                    connector_id,
                    results,
                    dag_run_id=dag_run_id,
                    component_type=component_type,
                )
                results['odoo_consumption_report'] = odoo_report
                print(f"Odoo consumption report: {odoo_report}")
            except Exception as report_exc:
                results['odoo_consumption_report'] = {
                    'status': 'failed',
                    'message': str(report_exc),
                }
                print(f"WARNING: Could not report consumption to Odoo: {report_exc}")

            # 5. Signal if we hit the limit during this run
            if synced_count >= remaining_quota and remaining_quota > 0:
                results['status'] = 'quota_reached'
                results['message'] = f"Sync truncated at {synced_count} records. Free quota exhausted."

            print(f"Sync completed successfully. Results: {results}")
            print("GOLDEN_SYNC_COMPLETE")
            return results

        except Exception as e:
            import traceback
            error_msg = f"Unexpected error during sync: {str(e)}"
            print(error_msg)
            print(traceback.format_exc())
            # Raise the exception so Airflow marks the task as failed
            raise

    process_sync()
