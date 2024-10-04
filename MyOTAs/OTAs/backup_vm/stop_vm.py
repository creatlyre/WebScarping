# %%
from azure.identity import ClientSecretCredential
from azure.mgmt.compute import ComputeManagementClient
import psutil


class StopVM():
    def __init__(self) -> None:
    # %%
        self.config = {
        "clientId": "7ef340dd-3d92-4e3f-9b4c-9d62889f4989",
        "clientSecret": "R3n8Q~sUUU190SpcqSEisohpi_-aTfB7Yi1pQdd2",
        "subscriptionId": "a0a8191f-5977-4098-b02c-bed6279bbd0a",
        "tenantId": "39ffbbb3-2e77-41c7-94df-5b52eef42062",
        "resourceGroupName": "MyOTAs",
        "virtualMachineName": "vm-win-mytoas-backup",
        "activeDirectoryEndpointUrl": "https://login.microsoftonline.com",
        "resourceManagerEndpointUrl": "https://management.azure.com/",
        "activeDirectoryGraphResourceId": "https://graph.windows.net/",
        "sqlManagementEndpointUrl": "https://management.core.windows.net:8443/",
        "galleryEndpointUrl": "https://gallery.azure.com/",
        "managementEndpointUrl": "https://management.core.windows.net/"
        }


    # %%
    def stop_vm(self): 
        # Replace the following with your Azure subscription ID
        subscription_id = self.config['subscriptionId']
        # Tenant ID from the service principal output
        tenant_id = self.config['tenantId']
        # Client ID from the service principal output
        client_id = self.config['clientId']
        # Client secret from the service principal output
        client_secret = self.config['clientSecret']

        # Resource group in which your VM is located
        resource_group_name = self.config['resourceGroupName']
        # Name of the VM you want to stop
        vm_name = self.config['virtualMachineName']

        # Create a credential object using the service principal
        credentials = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )

        # Create a Compute Management client
        compute_client = ComputeManagementClient(credentials, subscription_id)

        # Get the instance view of the VM
        # instance_view = compute_client.virtual_machines.instance_view(resource_group_name, vm_name)

        # # Extract and print the power state from the instance view status
        # vm_statuses = instance_view.statuses
        # power_state = next((s.code for s in vm_statuses if s.code.startswith('PowerState/')), 'Unknown')

        # print(f"The current power state of the VM '{vm_name}' is {power_state}.")

        # Stop the virtual machine
        async_vm_stop = compute_client.virtual_machines.begin_deallocate(resource_group_name, vm_name)
        async_vm_stop.result()

        print(f"The VM '{vm_name}' has been stopped.")


    # %%
    def check_if_script_is_running(script_name):
        # Iterate over all running processes
        for process in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Check if the process cmdline exists and contains the script name
                if process.info['cmdline'] and script_name in process.info['cmdline']:
                    return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                pass
        return False


# %%



