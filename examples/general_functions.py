import os
import subprocess as sp
import hashlib
import requests
import re
import json
from PIL import Image
import base64
from io import BytesIO
import imageio
import numpy as np
from datetime import datetime

from requests.auth import HTTPBasicAuth 
import time
import yaml


propdb_url = "https://foundry-admin.lbl.gov/api/json/"
propdb_orcid_endpoint = "sciCat-GetUser.aspx"
propdb_email_endpoint = "scicat/getuserbyemail.aspx"
propdb_proposal_endpoint = "PsyCat-GetProposal.aspx"
crucible_api_url = "https://crucible.lbl.gov/api"
gcs_config_name = "mf-cloud-storage"

def run_shell(cmd, checkflag = True, background = False):
    if background:
        return(sp.Popen(cmd, stdout = sp.PIPE, stderr = sp.STDOUT, shell = True, universal_newlines = True))
    return(sp.run(cmd, stdout = sp.PIPE, stderr = sp.STDOUT, shell = True, universal_newlines = True, check = checkflag))


def get_file_from_gcs(cloudpath, localpath, client_secret = None, creds = None):
    
    finalpath = os.path.join(localpath, cloudpath.split("/")[-1])
    
    if os.path.exists(finalpath):
        return(f"{finalpath} exists")
    
    cmd_args = get_rclone_command_line_args(client_secret, creds)
    
    if cmd_args is not None:
        if not cloudpath.startswith(":gcs:"):
            cloudpath = f":gcs:{cloudpath}"
        rclone_cmd = "   ".join(["rclone copy"] + cmd_args + [f"{cloudpath} {localpath}"])
    else:
        rclone_cmd = f"rclone copy mf-cloud-storage:/{cloudpath} {localpath}"

    e = run_shell(rclone_cmd, checkflag = False)
    assert os.path.exists(finalpath)


def get_secret(env_var, file_key): # to do make these match so you only need the one
    secret = os.getenv(env_var)
    if secret is None:
        try: 
            get_file_from_gcs("keys-and-certs/.secrets", "./")
        except Exception as err:
            print(f"[pycrucible:general_functions:get_secret] failed to get file from gcs with err {err}")
        with open(".secrets", "r") as f:
            secrets = yaml.safe_load(f)
        secret = secrets[file_key]
    return(secret)


def get_gcs_secret(secret_name, sa_creds):
    from google.cloud import secretmanager
    from google.oauth2 import service_account
    try:
        J = json.loads(os.getenv("GCS_SA"))
        with open("temp_creds.json", "w") as f:
            json.dump(J, f)
        credentials = service_account.Credentials.from_service_account_file("temp_creds.json")
        client = secretmanager.SecretManagerServiceClient(credentials=credentials)
        response = client.access_secret_version(name=secret_name)
        secret_value = response.payload.data.decode("UTF-8")
        return(secret_value)
    except Exception as err:
        print(err)
        
    credentials = service_account.Credentials.from_service_account_file(sa_creds)
    client = secretmanager.SecretManagerServiceClient(credentials=credentials)
    response = client.access_secret_version(name=secret_name)
    secret_value = response.payload.data.decode("UTF-8")
    return(secret_value)


def get_secret_2(env_var, gcs_secret_path= None, home_dir = os.getenv("HOME"), service_account_key = "mf-crucible-d2bdd66f0969.json"): 
    secret = os.getenv(env_var)
    if secret is None:
        crucible_config_folder = f"{home_dir}/.config/crucible/"
        #print(f"checking if credentials already exist in: {crucible_config_folder=}")
        if not os.path.exists(f"{crucible_config_folder}{service_account_key}"):
            #print(f"{crucible_config_folder}{service_account_key} not found  .... running get_file_from_gcs")
            get_file_from_gcs(f"keys-and-certs/{service_account_key}", crucible_config_folder)
        #print(f"[pycrucible:general_functions:get_secret_2] files found after copying credentials from storage {list_dir=}")
        
        secret = get_gcs_secret(gcs_secret_path,  f"{crucible_config_folder}{service_account_key}")
        os.environ[env_var] = secret
    return(secret)



def get_rclone_command_line_args(client_secret = None, creds = None):
    if client_secret is None:
        client_secret = os.getenv("GCS_CLIENT_SECRET")
    if creds is None:
        try: 
            home_dir = os.getenv("HOME")
            with open(f"{home_dir}/.config/mf-crucible-d2bdd66f0969.json", "r") as f:
                creds = f.read()
        except:
            creds = os.getenv("GCS_SA")

    if client_secret is not None and creds is not None:
        cmd_args = [
                f"--gcs-client-id=776258882599-v17f82atu67g16na3oiq6ga3bnudoqrh.apps.googleusercontent.com" ,
                f"--gcs-client-secret={client_secret}",
                f"--gcs-project-number=mf-crucible",
                f"--gcs-service-account-credentials='{creds}'",
                f"--gcs-object-acl=projectPrivate",
                f"--gcs-bucket-acl=projectPrivate",
                f"--gcs-env-auth=true"]
        
        return(cmd_args)
    
    else:
        return(None)

    

def send_file_to_gcs(localpath, cloudpath, client_secret = None, creds = None):
    
    cmd_args = get_rclone_command_line_args(client_secret, creds)
    
    if cmd_args is not None:
        if not cloudpath.startswith(":gcs:"):
            cloudpath = f":gcs:{cloudpath}"
        rclone_cmd = "   ".join(["rclone copy"] + cmd_args + [f"{localpath} {cloudpath}"])
    else:
        rclone_cmd = f"rclone copy {localpath} mf-cloud-storage:/{cloudpath}"

    e = run_shell(rclone_cmd).stderr 
    if e is not None:
        print(f"send_file_to_gcs({localpath}, {cloudpath}) failed")


def run_rclone_command(source_path= "", destination_path= "", cmd="copy", client_secret = None, creds = None, gcs_config_name = gcs_config_name, background = False):
    cmd_args = get_rclone_command_line_args(client_secret, creds)
    
    if len(destination_path.strip()) > 0:
            destination_path = f'"{destination_path}"'
    if cmd_args is not None:
        rclone_cmd = "   ".join([f'rclone {cmd}'] + cmd_args + [f'"{source_path}" {destination_path}'])
    else:
        source_path, destination_path = (x.replace(":gcs", gcs_config_name) for x in (source_path, destination_path))
        rclone_cmd = f'rclone {cmd} "{source_path}" {destination_path}'
    
    #print(f"[pycrucible:general_functions:run_rclone_command] using command {rclone_cmd}")
    run_shell_out = run_shell(rclone_cmd, background = background)
    # print(f'{run_shell_out.stdout=}')
    # print(f"{run_shell_out.stderr=}")
    return(run_shell_out)


def parse_rclone_config(rclone_config_filepath):
    # parse rclone config to json
    with open(rclone_config_filepath) as f:
        rclone_config_lines = [x.strip("\n") for x in f.readlines() if x != ""]
        
    drive_lines = [i for i,x in enumerate(rclone_config_lines) if x.startswith("[")]
    confd = {}
    for i in drive_lines:
        dname = rclone_config_lines[i].strip("[").strip("]")
        assert rclone_config_lines[i+1].startswith("type")
        if rclone_config_lines[i+1].endswith("drive"):
            n = 1
            confd[dname] = {}
            while n <= 6 and i+n < len(rclone_config_lines):
                next_line = rclone_config_lines[i+n]
                assert not next_line.startswith("[")
                kv = [x.strip() for x in next_line.split("=")]
                if len(kv) != 2:
                    print(kv)
                elif kv[0] == "client_secret":
                    skip = True
                else:
                    confd[dname][kv[0]] = kv[1]
                n+=1
    with open("rclone_config.json", "w") as f:
        json.dump(confd, f)
    return(confd)



def mdpath_check(dsfile):
    parts = dsfile.split("/")
    folder = parts[-2]
    file_ = parts[-1].strip(".json")
    conditions = [folder == file_, dsfile.endswith(".json"), 'queue' not in dsfile, 'transfer' not in dsfile]

    return(all(conditions))


def check_exists(objid, idlist):
    return(objid in idlist)


def checkhash(file):
    with open(file,"rb") as f:
        fdata = f.read() 
        readable_hash = hashlib.sha256(fdata).hexdigest()
    return(readable_hash)


def check_orcid_entry(orcid):
    print(orcid)
    clean_orcid = orcid.replace("-", "").strip()
    pattern= re.compile("[0-9A-Z]{16}$")
    if re.match(pattern, clean_orcid):
        remake_orcid = "-".join([clean_orcid[0:4], clean_orcid[4:8], clean_orcid[8:12], clean_orcid[12:16]])
        print(remake_orcid)
        return(remake_orcid)
    else:
        return("XXXX-XXXX-XXXX-XXXX")


def get_any(d, keys, default=None):
    for k in keys: 
        if k in d:
           return d[k]
    return default

    
def build_b64_thumbnail(image: Image, tnpath, max_size = (400,400)): 
        image.thumbnail(max_size)
        image.convert("RGB")
        buffered = BytesIO()
        image.save(buffered, format="PNG")
        image.save(tnpath, format = "PNG")
        thumbnail = base64.b64encode(buffered.getvalue()).decode("UTF-8")
        return(thumbnail)


class ScopeFoundryJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.int16):
            return int(obj)
        if isinstance(obj, np.int32):
            return int(obj)
        if isinstance(obj, np.float32):
            return float(obj)
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, np.float64):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.uint8):
            return int(obj)
        if isinstance(obj, np.uint32):
            return int(obj)
        if isinstance(obj, np.uint64):
            return int(obj)
        if isinstance(obj, datetime):
            return(str(obj.isoformat()))
        return json.JSONEncoder.default(self, obj)


def get_sql_user_by_orcid(orcid, baseurl = crucible_api_url):
    apikey = get_secret_2("ADMIN_APIKEY", "projects/776258882599/secrets/crucible_admin_apikey/versions/1")
    user = requests.request(method="get", url=f"{baseurl}/users/{orcid}", headers = {"Authorization":f"Bearer {apikey}"})
    
    try:
        return(user.json())
    except:
        return(None)


def translate_proposal_to_drives(proposal, owner_orcid, lost_users = {"kclofgre":"kevinlofgren"}):
    proposal = proposal.strip()

    if proposal in ["unknown", ""]:
        drive_names = ["Unclaimed"]
    
    elif "USER" in proposal:
        user = proposal.split("_")[1].lower().strip()
        if user in lost_users.keys():
            user = lost_users[user]
            
        drive_names = [f"MFUSER_{user}"]
        
    elif "MFP" in proposal:
        print(owner_orcid)
        user = get_sql_user_by_orcid(owner_orcid)
        print(user)
        if user is not None:
            user_email = user['lbl_email'] if user['lbl_email'] is not None else None
        else:
            user_email = None
            
        if user_email is not None:
            user_id = user_email.split("@")[0].lower().strip()
            drive_name = f"MFUSER_{user_id}"
            drive_names = [proposal, drive_name]

        else:
            drive_names = [proposal]


    elif "CFNP" in proposal:
        print(owner_orcid)
        user = get_sql_user_by_orcid(owner_orcid)
        print(user)
        if user is not None:
            user_email = user['email'] if user['email'] is not None else None
        else:
            user_email = None
            
        if user_email is not None:
            user_id = user_email.split("@")[0].lower().strip()
            drive_name = f"CFNUSER_{user_id}"
            drive_names = [proposal, drive_name]
            return([proposal, drive_name])
        else:
            drive_names = [proposal]
            return([proposal])
            
    else:  
        drive_names = [proposal]
        
    return(drive_names)


def get_drive_id_from_proposal(proposal, baseurl = crucible_api_url):
    apikey = get_secret_2("ADMIN_APIKEY", "projects/776258882599/secrets/crucible_admin_apikey/versions/1")
    ds = requests.request(method="get", url=f"{baseurl}/driveid_by_proposal/{proposal}", headers = {"Authorization":f"Bearer {apikey}"})
    try:
        print(f"{ds.json()=}")
        return(ds.json())
    except Exception as err:
        print(f"Error getting drive ID from api [pycrucible.general_functions.get_drive_id_from_proposal line 294]: {err}")
        


# prop db calls
def get_propdb_info(url, endpoint, searchby, searchval):
    apikey = get_secret("PROPDB_APIKEY", "prop_db_apikey")
    request_url = f"{url}{endpoint}?key={apikey}&{searchby}={searchval}"
    assert apikey is not None
    print(f"{url}{endpoint}?key=****&{searchby}={searchval}")
    response = requests.request(method="get", url=request_url)
    if len(response.json()) > 0 and response.status_code == 200:
        return(response.json()) # single dictionary
    else:
        return(None)
    
# backwards compatibility but this function is MF specific
get_mf_propdb_info = get_propdb_info

def parse_email(all_user_accounts, provided_email=""):
    emails = {acct["email"]:acct['lbl_email'].lower() for acct in all_user_accounts if acct['lbl_email'] is not None}
    num_lbl_emails = len(set(list(emails.values())))
    
    if num_lbl_emails == 1:
        use_this_email = list(emails.values())[0]
    elif num_lbl_emails > 1 and provided_email != "":
        use_this_email = emails[provided_email]
    elif num_lbl_emails > 1 and provided_email == "":
        use_this_email = list(emails.values())[0]
    else:
        use_this_email = provided_email
        
    return(use_this_email)


def collect_user_accounts(user_info):
    try:
        all_user_accounts = get_propdb_info(propdb_url, propdb_orcid_endpoint, searchby = "orcid", searchval = user_info['orcid'])
    except:
        all_user_accounts = [user_info]
    return(all_user_accounts)


def parse_proposals(all_user_accounts, email):
    internal_research_val = f"MFUSER_{email.split('@')[0]} (InternalResearch)"
    proposal_list = []
    for acct in all_user_accounts:
        proposal_list += acct['proposals']
    proposal_list += [internal_research_val]
    return(proposal_list)
   


def parse_username(all_user_accounts):
    usernames = [f"{acct['first_name']} {acct['last_name']}" for acct in all_user_accounts if acct['last_name'] is not None]
    num_names = len(set(usernames))
    print(set(usernames))
    if num_names == 1:
        return(usernames[0])
    elif num_names > 1:
        return('multiple names found, please type your first and last name manually')
    else:
        return("no names found for this orcid")
