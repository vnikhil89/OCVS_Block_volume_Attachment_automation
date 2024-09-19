import oci
import logging
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import time
import os
import json
from fdk import response
import io
import base64

try:
    # Oracle Cloud Instance and Volume Details
    root_compartment_id = os.getenv("ROOT_COMPARTMENT_OCID")  # Use an empty string to start from the root compartment
    compartment_name = os.getenv("Compartment_name") # Update with your compartment name
    ad_name = os.getenv("ad_name")
    vcenter_ip = os.getenv("vcenter_ip")
    combined_secret_ocid = os.getenv("combined_secret_ocid")
    if not root_compartment_id:
      raise ValueError("ERROR: Missing configuration key root_compartment_id")
    if not compartment_name:
      raise ValueError("ERROR: Missing configuration key compartment_name")
    if not ad_name:
      raise ValueError("ERROR: Missing configuration key ad_name")
    if not vcenter_ip:
      raise ValueError("ERROR: Missing configuration key vcenter_ip")
    if not combined_secret_ocid:
      raise ValueError("ERROR: Missing configuration key combined_secret_ocid")
     

    signer = oci.auth.signers.get_resource_principals_signer()
    compute_client = oci.core.ComputeClient(config={},signer=signer)
    block_volume_client = oci.core.BlockstorageClient(config={},signer=signer)
    identity_client = oci.identity.IdentityClient(config={},signer=signer)
    secret_client = oci.secrets.SecretsClient(config={},signer=signer)

except Exception as e:
   logging.getLogger().error(e)
   raise


def get_compartment_id_by_name(identity_client, compartment_name, root_compartment_id=""):
    """
    Retrieves the compartment OCID by its name.
    """
    print(f"Retrieving compartment ID for name {compartment_name}...")

    # List all compartments
    compartments = identity_client.list_compartments(
        compartment_id=root_compartment_id,
        compartment_id_in_subtree=True
    ).data

    # Filter compartments by name
    for compartment in compartments:
        if compartment.name == compartment_name:
            print(f"Found compartment: {compartment.name} with OCID: {compartment.id}")
            return compartment.id

    print(f"No compartment found with name {compartment_name}")
    return None

def get_ad_ocid_by_name(identity_client, compartment_id, ad_name):

    try:
        # List all availability domains in the specified compartment
        ads = identity_client.list_availability_domains(compartment_id).data
        
        # Iterate through the list to find the AD with the given name
        for ad in ads:
            if ad.name == ad_name:
                return ad.id  # Return the OCID of the matching AD
        
        # If the AD name is not found, return None
        print(f"Availability Domain with name '{ad_name}' not found.")
        return None
    
    except oci.exceptions.ServiceError as e:
        print(f"Service error: {e}")
        return None

def get_instances_by_shape(compute_client, compartment_id, shape):
    """
    Retrieves a list of instance IDs filtered by shape.
    """
    print(f"Retrieving instances with shape {shape}...")

    # List all instances in the specified compartment
    instances = compute_client.list_instances(compartment_id).data

    # Filter instances by the specified shape
    filtered_instances = [instance.id for instance in instances if instance.shape == shape]

    if not filtered_instances:
        print(f"No instances found with shape {shape}")
    else:
        print(f"Found {len(filtered_instances)} instances with shape {shape}")
    
    return filtered_instances

def get_block_volume_by_name(block_volume_client, compartment_id, block_volume_name):
    """
    Retrieves the block volume OCID by its name.
    """
    print(f"Retrieving block volume with name {block_volume_name}...")

    # List all block volumes in the specified compartment
    
    print(f"Compartment OCID: {compartment_id}")
    volumes_data = block_volume_client.list_volumes(compartment_id=compartment_id)
    volumes = volumes_data.data
    # Filter the volumes by name
    for volume in volumes:
        if volume.display_name == block_volume_name:
            print(f"Found block volume: {volume.display_name} with OCID: {volume.id}")
            return volume.id

    print(f"No block volume found with name {block_volume_name}")
    return None

def get_secret_value(secret_client, secret_id):
    try:
        response = secret_client.get_secret_bundle(secret_id)
        secret_content = response.data.secret_bundle_content.content
        decoded_secret = base64.b64decode(secret_content).decode("utf-8")
        return decoded_secret
    except oci.exceptions.ServiceError as e:
        print(f"Failed to retrieve secret: {e}")
        raise

def parse_credentials(secret_value):
    try:
        username, password = secret_value.split('/', 1)
        return username, password
    except ValueError:
        print("Failed to parse secret value. Ensure it is in 'username/password' format.")
        raise

def attach_oci_block_volume(compute_client, volume_id, instance_id, availability_domain):
    """
    Attaches an OCI block volume to an instance and returns the attachment info.
    """
    print(f"Attaching OCI block volume {volume_id} to instance {instance_id} in {availability_domain}...")
    attach_details = oci.core.models.AttachIScsiVolumeDetails(
        display_name="iSCSI-Attachment",
        instance_id=instance_id,
        volume_id=volume_id,
        type="iscsi"
    )

    response = compute_client.attach_volume(attach_details)
    attachment_id = response.data.id
    while True:
        attachment = compute_client.get_volume_attachment(attachment_id).data
        if attachment.lifecycle_state == 'ATTACHED':
            print("Volume successfully attached.")
            return attachment
        elif attachment.lifecycle_state == 'DETACHED':
            print("Volume is detached. Retry the attachment.")
            return None
        else:
            print("Waiting for attachment to complete...")
            time.sleep(10) 


def get_iscsi_target_info(compute_client, volume_attachment_id):
    """
    Retrieve the iSCSI target information from the OCI block volume.
    """
    print(f"Getting iSCSI target information for attachment {volume_attachment_id}...")
    volume_attachment = compute_client.get_volume_attachment(volume_attachment_id).data
    iscsi_targets = volume_attachment.iqn, volume_attachment.ipv4, volume_attachment.port

    print(f"iSCSI target IP: {volume_attachment.ipv4}, Port: {volume_attachment.port}, IQN: {volume_attachment.iqn}")
    return iscsi_targets

def rescan_iscsi_adapter(host, iscsi_adapter):
    """
    Rescans the iSCSI adapter on the ESXi host.
    """
    print(f"Rescanning iSCSI adapter {iscsi_adapter.device} on host {host.name}...")
    try:
        # Perform the rescan
        host.configManager.storageSystem.RescanHba(iscsi_adapter.device)
        print("iSCSI adapter rescan completed successfully.")
    except Exception as e:
        print(f"Failed to rescan iSCSI adapter {iscsi_adapter.device}: {e}")

def attach_iscsi_target_to_esxi(host, iscsi_target_ip, iscsi_target_port, iscsi_name):
    """
    Attaches an iSCSI target to an ESXi host.
    """
    print(f"\nAttaching iSCSI target to ESXi host: {host.name}")

    # Get the host's storage system
    storage_system = host.configManager.storageSystem

    # Get iSCSI software adapters (InternetScsiHba)
    hba_list = storage_system.storageDeviceInfo.hostBusAdapter
    iscsi_adapter = None
    for hba in hba_list:
        if isinstance(hba, vim.host.InternetScsiHba):
            iscsi_adapter = hba
            print(f"Found iSCSI adapter: {iscsi_adapter.device}")
            break

    if not iscsi_adapter:
        print(f"No iSCSI adapter found on host {host.name}. Skipping...")
        return

    # Create the SendTarget object with the new target IP and port
    send_target = vim.host.InternetScsiHba.SendTarget(address=iscsi_target_ip, port=iscsi_target_port)

    # Attach the iSCSI target to the adapter
    try:
        storage_system.AddInternetScsiSendTargets(iscsi_adapter.device, [send_target])
        print(f"Successfully attached iSCSI target {iscsi_target_ip}:{iscsi_target_port} to {host.name}")

        rescan_iscsi_adapter(host, iscsi_adapter)
    except Exception as e:
        print(f"Failed to attach iSCSI target to {host.name}: {e}")

def get_all_esxi_hosts(content):
    """
    Helper function to retrieve all ESXi hosts in the vCenter/ESXi environment.
    """
    container = content.viewManager.CreateContainerView(content.rootFolder, [vim.HostSystem], True)
    esxi_hosts = container.view
    container.Destroy()
    return esxi_hosts


def attach_iscsi_target_to_all_esxi_hosts(vcenter_ip, username, password, iscsi_target_ip, iscsi_target_port, iscsi_name):
    """
    Connect to vCenter and attach the iSCSI target to all ESXi hosts.
    """
    # Disable SSL certificate verification for demo purposes
    context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
    context.verify_mode = ssl.CERT_NONE

    # Connect to vCenter/ESXi
    si = SmartConnect(host=vcenter_ip, user=username, pwd=password, sslContext=context)

    try:
        content = si.RetrieveContent()

        # Get all ESXi hosts
        esxi_hosts = get_all_esxi_hosts(content)
        print(f"Found {len(esxi_hosts)} ESXi hosts in the environment.")

        # Loop through each ESXi host and attach the iSCSI target
        for host in esxi_hosts:
            attach_iscsi_target_to_esxi(host, iscsi_target_ip, iscsi_target_port, iscsi_name)

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        Disconnect(si)


# Main Function
def handler(ctx, data: io.BytesIO = None):
    try:
        body = json.loads(data.getvalue())
        shape = body["shape"]
        block_volume_name = body["block_volume_name"]
        compartment_id = get_compartment_id_by_name(identity_client, compartment_name, root_compartment_id)
        if not compartment_id:
            print("Exiting: Compartment with specified name not found.")
            return
    
        # Get all instance IDs with the specified shape
        instance_ids = get_instances_by_shape(compute_client, compartment_id, shape)
    
        # Get block volume ID by name
        volume_id = get_block_volume_by_name(block_volume_client, compartment_id, block_volume_name)
        if not volume_id:
            print("Exiting: Block volume with specified name not found.")
            return
    
        # Loop through each instance and attach the block volume
        availability_domain = get_ad_ocid_by_name(identity_client, compartment_id, ad_name)
        for instance_id in instance_ids:
            attachment_info = attach_oci_block_volume(compute_client, volume_id, instance_id, availability_domain)
            iscsi_iqn, iscsi_ip, iscsi_port = get_iscsi_target_info(compute_client, attachment_info.id)
            combined_secret = get_secret_value(secret_client, combined_secret_ocid)
            vcenter_username, vcenter_password = parse_credentials(combined_secret)            
    
            # Attach iSCSI target to all ESXi hosts
            attach_iscsi_target_to_all_esxi_hosts(vcenter_ip, vcenter_username, vcenter_password, iscsi_ip, iscsi_port, iscsi_iqn)

    except Exception as handler_error:
        logging.getLogger().error(handler_error)

    return response.Response(
        ctx, 
        response_data=json.dumps({"status": "Success"}),
        headers={"Content-Type": "application/json"}
    )