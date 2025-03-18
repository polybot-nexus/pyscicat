# libraries
import json
import pyscicat.client as pyScClient
import pyscicat.model as pyScModel
from pyscicat.client import encode_thumbnail, ScicatClient
from pyscicat.model import Attachment, Datablock, DataFile, Dataset, Ownable, OrigDatablock

# scicat configuration file
# includes scicat instance URL
# scicat user and password
scicat_configuration_file = r"C:\Users\avriza\Desktop\pyscicat\examples\data\ingestion_simulation_dataset_ess_config.json"
simulation_dataset_file = r"C:\Users\avriza\Desktop\pyscicat\examples\data\ingestion_simulation_dataset_ess_raw_dataset.json"   #"./data/ingestion_simulation_dataset_ess.json"


# loads scicat configuration
with open(scicat_configuration_file, "r") as fh:
    scicat_config = json.load(fh)


# loads simulation information from matching json file
with open(simulation_dataset_file, "r") as fh:
    dataset_information = json.load(fh)

# instantiate a pySciCat client
# scClient = pyScClient.ScicatClient(
#     base_url=scicat_config["scicat"]["host"],
#     username=scicat_config["scicat"]["username"],
#     password=scicat_config["scicat"]["password"],
# )

scClient = ScicatClient(
    base_url="http://localhost:3000/api/v3", username="ingestor", password="aman"
)
print('scicat', scClient)

# create an owneable object to be used with all the other models
# all the fields are retrieved directly from the simulation information
ownable = pyScModel.Ownable(**dataset_information["ownable"])


# create dataset object from the pyscicat model
# includes ownable from previous step
dataset = pyScModel.RawDataset(**dataset_information["dataset"], **ownable.dict())


# create dataset entry in scicat
# it returns the full dataset information, including the dataset pid assigned automatically by scicat
created_dataset = scClient.upload_new_dataset(dataset)
print('created_dataset', created_dataset)

# # create origdatablock object from pyscicat model
origDataBlock = pyScModel.OrigDatablock(
    size=dataset_information["orig_datablock"]["size"],
    datasetId=created_dataset,
    dataFileList=[
        pyScModel.DataFile(**file)
        for file in dataset_information["orig_datablock"]["dataFileList"]
    ],
    **ownable.dict()
)

# # create origDatablock associated with dataset in SciCat
# # it returns the full object including SciCat id assigned when created
created_orig_datablock = scClient.upload_dataset_origdatablock(dataset_id=created_dataset, datablockDto=origDataBlock)

# data_file = DataFile(path="file.h5", size=42)
# data_block = OrigDatablock(
#     size=42, version='1', datasetId=created_dataset, dataFileList=[data_file], **ownable.dict()
# )
# print(data_block.__dict__)
# scClient.upload_dataset_origdatablock(dataset_id=created_dataset, datablockDto=data_block)