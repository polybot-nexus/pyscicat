from datetime import datetime
import json
import numpy as np

import base64
import requests
# from requests.auth import HTTPBasicAuth 

from general_functions import run_shell, ScopeFoundryJSONEncoder, get_secret_2
from pyscicat.client import from_token, from_credentials, ScicatClient
from pyscicat.model import Ownable, RawDataset, DerivedDataset, CreateDatasetOrigDatablockDto, Attachment, DataFile, Proposal, Instrument



def setup_scicat_client():
    # if "PROD" in scicat_pw_var:
    #     secret_path = 'projects/776258882599/secrets/scicat-admin-password-prod/versions/2'
    # else:
    #     secret_path = 'projects/776258882599/secrets/scicat-admin-password-testenv/versions/1'
    # scicat_pw = get_secret_2(scicat_pw_var, secret_path)
    scicat_client = ScicatClient(
         base_url="http://localhost:3000/api/v3", username="ingestor", password="aman")
    print(scicat_client)
    return(scicat_client)


def get_all_sc(scicat_url, scicat_client, endpoint_ext):
    response = requests.request(
                    method="get",
                    url=f"{scicat_url}{endpoint_ext}/",
                    params={"access_token": scicat_client._token},
                    headers={"Authorization": f"Bearer {scicat_client._token}"}
                    )
    if response.status_code != 200:
        raise ValueError(response)
    else:
        return(response.json())


def delete_items(scicat_url, scicat_client, endpoint_ext):
    delout = requests.request(
        method="delete",
        url=f"{scicat_url}{endpoint_ext}",
        params={"access_token": scicat_client._token},
        headers={"Authorization": f"Bearer {scicat_client._token}"})
    return(delout.json())


def delete_existing_for_overwrite(found_datasets, scicat_client):
    scicat_url = scicat_client._base_url
    for eachds in found_datasets:
        dsid = eachds['id'].replace("/", "%2F")
        attachments = [x['_id'] for x in get_all_sc(scicat_url, scicat_client, f"datasets/{dsid}/attachments")]
        datablocks = [x['_id'] for x in get_all_sc(scicat_url, scicat_client, f"datasets/{dsid}/origdatablocks")]

        # delete everything
        print("deleting attachments")
        for aid in attachments: 
            delete_items(scicat_url, scicat_client, f"datasets/{dsid}/attachments/{aid}")

        print("deleting datablocks")
        for dbid in datablocks: 
            dbout = delete_items(scicat_url, scicat_client, f"datasets/{dsid}/origdatablocks/{dbid}")
            print(dbout)

        print("deleting dataset")
        delete_items(scicat_url, scicat_client, f"datasets/{dsid}")
        

class ScicatDataSetUploader(object):

    def __init__(self, jsonfile, dtype = "raw"):
        
        self.delete_from_scicat = False
        self.dtype = dtype
        self.google_drive_info = []
        
        with open(jsonfile, "r") as f:
            import_metadata = json.load(f)
        import_attributes= ['source_folder',
                            'overwrite_data', 
                            'owner',
                            'orcid',
                            'unique_id',
                            'dataset_name', 
                            'local_source_folder',
                            'email',
                            'creation_location',
                            'instrument_name',
                            'proposal', 
                            'access_groups', 
                            'principal_investigator',
                            'size', 
                            'creation_time',
                            'data_format',
                            'keywords',
                            'metadata_dictionary',
                            'associated_files', 
                            'thumbnails', 
                            'google_drive_info',
                            'job_parameters', 
                            'repo', 
                            'cloudbuild_link',
                            'raw_data_uuid', 
                            'public',
                            'google_drive_info',
                            'measurement'
                           ]

        
        for attr in import_attributes:
            if attr in import_metadata.keys():
                setattr(self, attr, import_metadata[attr]) 
            if attr == "email":
                self.email = self.email.replace(" ", "_")
        
     
    def check_dataset_catalog_status(self, scicat_client, extension = ""):
        dataset_name = self.dataset_name.replace(extension, "")
        found_datasets = scicat_client.datasets_find(query_fields={"pid": self.unique_id})
        # if len(found_datasets) < 1:
        #     found_datasets = scicat_client.datasets_find(query_fields={"datasetName": dataset_name})
        if self.overwrite_data == True and len(found_datasets) > 0:
            self.delete_from_scicat = True
        return(found_datasets)

    
    def build_ownable_from_patch(self):
        if not hasattr(self, 'owner'):
            self.owner = self.instrument_name
        
        if not hasattr(self, 'access_groups'):
            self.access_groups = [self.owner] 
            
        if not hasattr(self, 'proposal'):
            self.proposal = 'unknown'
            
        if not hasattr(self, 'email'):
            self.email = f'{self.owner.replace(" ", "_")}@lbl.gov'
            
        # try:
        #     owner_group = self.orcid if self.orcid is not None else self.owner
        # except:
        owner_group = self.owner
        self.ownable = Ownable(ownerGroup=owner_group, accessGroups=self.access_groups)
        self.principal_investigator = self.owner
    
  
    def find_or_add_proposal(self, scicat_client):
        scicat_url = scicat_client._base_url
        search_results = scicat_client.proposals_get_one(self.proposal)
        
        if search_results is not None:
            return("proposal exists")
            
        elif self.proposal.startswith("MFP"):
            prop_obj = self.define_external_research_proposal()
    
        elif self.proposal.startswith("MFUSER"):
            prop_obj = Proposal(proposalId = self.proposal,
                                email = self.email,
                                title = "InternalResearch",
                                **self.ownable.model_dump())
        else:
            prop_obj = Proposal(proposalId = self.proposal,
                                email = self.email,
                                title = "unknown",
                                **self.ownable.model_dump())

        if prop_obj is not None:
            print(f"creating proposal {prop_obj}")
            prop_id = scicat_client.proposals_create(prop_obj)
            print(f"DATA ADDED FOR A NEW PROPOSAL - {prop_id} - make sure users have access")
            return(prop_id)
        else:
            self.proposal = "unknown"




    # THIS HAS TO BE UPDATED 
    def define_external_research_proposal(self, baseurl = "https://crucible.lbl.gov/api"):
    
        apikey = get_secret_2("ADMIN_APIKEY", "projects/776258882599/secrets/crucible_admin_apikey/versions/1")
        
        try:
            prop_response = requests.request(method="get", url=f"{baseurl}/proposal/{self.proposal}", headers = {"Authorization":f"Bearer {apikey}"}).json()
        except:
            prop_response = None
        
        print(f"{prop_response=}")

        if prop_response:
            prop_access = [x['orcid'] for x in requests.request(method="get", url=f"{baseurl}/users_on_proposal/{self.proposal}", headers = {"Authorization":f"Bearer {apikey}"}).json()]
            prop_lead_email = prop_response['assigned_scientist_email'].lower().strip()

            print(prop_lead_email)
            try:
                prop_lead_info = requests.request(method="get", 
                                                  url=f"{baseurl}/users_by_email/{prop_lead_email}", 
                                                  headers = {"Authorization":f"Bearer {apikey}"}).json()[0]
                prop_access.append(prop_lead_info['orcid'])
                email = prop_lead_info['lbl_email'] if prop_lead_info['lbl_email'] is not None else prop_lead_info['email']
                owner = prop_lead_info['orcid']
                
            except:
                print(f"{prop_lead_email} not found in SQL user table")
                email = prop_lead_email
                owner = prop_lead_email.split("@")[0]
                
            # create proposal object
            prop_obj = Proposal(proposalId = self.proposal,
                                email = email,
                                title = prop_response['title'],
                                ownerGroup = owner,
                                accessGroups = prop_access)
            return(prop_obj)
        else:
            print(f"{self.proposal} not found in SQL proposal table")
            return(prop_response)
        


    
    def find_or_add_instrument(self, scicat_client):
        scicat_url = scicat_client._base_url
        
        found = get_all_sc(scicat_url, scicat_client, "instruments")
        found_instruments = {x['uniqueName']: x['pid'] for x in found}     
        instrument_exists = self.instrument_name in list(found_instruments.keys())
        if instrument_exists:
            instrument_id = found_instruments[self.instrument_name]
        elif self.instrument_name == "":
            self.instrument_id = None
        else:
            instrument_obj = Instrument(uniqueName = self.instrument_name, name = self.instrument_name)
            instrument_id = scicat_client.instruments_create(instrument_obj)
        self.instrument_id = instrument_id
        return(instrument_id)


    def add_colab_links(self):
        insitu_pl_notebook_links = ["https://github.com/MolecularFoundry/crucible-analysis-notebooks/blob/main/insitu-analysis/InSituPL_Fitting.ipynb"]
        insitu_uvvis_notebook_links = ["https://github.com/MolecularFoundry/crucible-analysis-notebooks/blob/main/insitu-analysis/InSituUVvis_Fitting.ipynb"]
        tem_notebook_links = ["https://github.com/MolecularFoundry/crucible-analysis-notebooks/blob/main/Crucible_TEM_Viewer.ipynb"]
        hyperspec_notebook_links = ["https://github.com/MolecularFoundry/crucible-analysis-notebooks/blob/main/hyperspec-analysis/Hyperspectral_Dimensional_Reduction_PCA.ipynb", "https://github.com/MolecularFoundry/crucible-analysis-notebooks/blob/main/hyperspec-analysis/Hyperspectral_Explorer.ipynb"]
        colab_links = []
        if self.measurement in ['In Situ PL']:
            colab_links += insitu_pl_notebook_links
        if self.measurement in ['In Situ UV-Vis']:
            colab_links += insitu_uvvis_notebook_links
        if self.data_format in ["emd", "dm3", "dm4", "ser"]:
            colab_links += tem_notebook_links
        if self.measurement in ["hyperspec_picam_mcl"]:
            colab_links += hyperspec_notebook_links
        if len(colab_links) > 0:
            colab_link_html = "<br>".join(["<br>Explore in Colab:"] + colab_links)
        else:
            colab_link_html = ""
        return(colab_link_html)
    
    
    def to_scicat(self, scicat_client): 
        try:
            if self.instrument_name == 'bioglow':
                self.metadata_dictionary['measurement']['bioglow_spec']['countrate_blocks'] = None
                self.metadata_dictionary['measurement']['bioglow_spec']['record_blocks'] = None
            elif self.instrument_name == 'JupiterAFM':
                self.metadata_dictionary['wData'] = None
            else:
                print("uploading metadata")
        except Exception as err:
            print(err)
        
        new_md = {k:v for k,v in self.metadata_dictionary.items() if v is not None}
        data = json.dumps(new_md, cls = ScopeFoundryJSONEncoder)
        try:
            public_status = self.public
        except:
            public_status = False
        validated_kw = [x for x in self.keywords if x not in ["list", "tags", "separated", "by", "commas", "(Optional)"]]
        colab_link_html = self.add_colab_links()
        gdrive_links = [f"{x['drive_name']}: {x['url']}" for x in self.google_drive_info if x['status'] == 'current' and 'Organized' in x['folder_path_in_drive']]
        description_links = "<br>".join(gdrive_links) + "<br>" + colab_link_html
        new_dataset = RawDataset(sourceFolder = self.source_folder,
                                 owner = self.owner,
                                 pid = self.unique_id,
                                 datasetName = self.dataset_name,
                                 contactEmail = self.email,
                                 creationLocation = self.creation_location,
                                 instrumentId = self.instrument_id,
                                 proposalId = self.proposal,
                                 principalInvestigator = self.principal_investigator,
                                 scientificMetadata = json.loads(data),
                                 size = self.size,
                                 type = self.dtype,
                                 creationTime = self.creation_time,
                                 dataFormat = self.data_format,
                                 keywords= validated_kw,
                                 isPublished = public_status,
                                 description = description_links, 
                                 **self.ownable.model_dump())
        

        print("running dataset create")
        print(f"{new_dataset=}")
        ds_create_out = scicat_client.datasets_create(new_dataset)

        run_shell(f"rm {self.unique_id}.json")


    # new in subclass
    def to_scicat_derived_dataset(self, scicat_client): 
        job_params = json.dumps(self.job_parameters)
        sci_md = json.dumps(self.metadata_dictionary, cls = ScopeFoundryJSONEncoder)
        print(f"{self.orcid=}")
        try:
            public_status = self.public
        except:
            public_status = False
        #data = json.dumps(self.metadata_dictionary, cls = ScopeFoundryJSONEncoder)
        new_dataset = DerivedDataset(
                                 # required for all datasets
                                 owner = self.owner,
                                 ownerGroup = self.orcid if self.orcid is not None else self.owner,
                                 contactEmail = self.email,
                                 creationTime = self.creation_time,
                                 sourceFolder = self.source_folder,
            
                                 # required for derived
                                 investigator = self.principal_investigator,
                                 inputDatasets = [self.raw_data_uuid],
                                 usedSoftware = [self.repo],
    
                                 # optional
                                 pid = self.unique_id,
                                 accessGroups = self.access_groups,
                                 datasetName = self.dataset_name,
                                 jobParamaters = json.loads(job_params),
                                 jobLogData = self.cloudbuild_link,
                                 type = "derived",
                                 scientificMetadata = json.loads(sci_md),
                                 isPublished = public_status,
                                 size = self.size,
                                 keywords= self.keywords)
    
        
        dataset_unique_id = scicat_client.datasets_create(new_dataset)
        run_shell(f"rm {self.unique_id}.json")
        print(dataset_unique_id)


    def to_scicat_updateonly(self, scicat_client): 
        try:
            if self.instrument_name == 'bioglow_spec':
                self.metadata_dictionary['measurement']['bioglow_spec']['countrate_blocks'] = None
                self.metadata_dictionary['measurement']['bioglow_spec']['record_blocks'] = None
            elif self.instrument_name == 'JupiterAFM':
                self.metadata_dictionary['wData'] = None
            else:
                pass
        except Exception as err:
            print(err)

        
        try:
            public_status = self.public
        except:
            public_status = False
            
        data = json.dumps(self.metadata_dictionary, cls = ScopeFoundryJSONEncoder)
        print(f"{self.google_drive_info=}")
        colab_link_html = self.add_colab_links()
        gdrive_links = [f"{x['drive_name']}: {x['url']}" for x in self.google_drive_info if x['status'] == 'current' and 'Organized' in x['folder_path_in_drive']]
        description_links = "<br>".join(gdrive_links) + "<br>" + colab_link_html
        validated_kw = [x for x in self.keywords if x not in ["list", "tags", "separated", "by", "commas", "(Optional)"]]
        new_dataset = RawDataset(sourceFolder = self.source_folder,
                                 owner = self.owner,
                                 datasetName = self.dataset_name,
                                 contactEmail = self.email,
                                 creationLocation = self.creation_location,
                                 instrumentId = self.instrument_id,
                                 proposalId = self.proposal,
                                 principalInvestigator = self.principal_investigator,
                                 scientificMetadata = json.loads(data),
                                 size = self.size,
                                 type = self.dtype,
                                 creationTime = self.creation_time,
                                 dataFormat = self.data_format,
                                 keywords= validated_kw,
                                 isPublished = public_status,
                                 description = description_links, 
                                 **self.ownable.model_dump())


        
        ds_update_out = scicat_client.datasets_update(new_dataset, self.unique_id)
                    
        run_shell(f"rm {self.unique_id}.json")

    

    def catalog_additional_files(self, scicat_client):
        # TODO rename to scicat_catalog_files?
        data_files = []
        totalsize = 0
        dataset_id = self.unique_id
        for each_file in self.associated_files.keys():
            fsize = self.associated_files[each_file]['size']
            totalsize += fsize
            if self.dtype == "raw":
                ndf = DataFile(path = each_file.replace(self.local_source_folder, self.source_folder), 
                                     size = fsize, 
                                     time = datetime.now().isoformat(), 
                                     chk = self.associated_files[each_file]['sha256_hash'],
                                     type = "RawDatasets" ) 
            else:
                ndf = DataFile(path = each_file, 
                                     size = fsize, 
                                     time = datetime.now().isoformat(), 
                                     chk = self.associated_files[each_file]['sha256_hash'],
                                     type = "DerivedDatasets") 
            data_files += [ndf]
                          
    
        # what data blocks
        data_block = CreateDatasetOrigDatablockDto(size = totalsize,
                                                   datasetId = dataset_id,
                                                   dataFileList = data_files)
        scicat_client.upload_dataset_origdatablock(dataset_id, data_block)
        return("upload complete")


    def upload_thumbnail_attachments(self, scicat_client):
        # rename to scicat_upload_thumbnail_attachments
        imType = "png"
        header = "data:image/{imType};base64,".format(imType=imType)
        
        for x in self.thumbnails:
            attachment = Attachment(datasetId = self.unique_id, \
                                    thumbnail=header+x['thumbnail'], \
                                    caption = x['caption'], \
                                    ownerGroup=self.owner, accessGroups=self.access_groups)
            scicat_client.upload_attachment(attachment)
