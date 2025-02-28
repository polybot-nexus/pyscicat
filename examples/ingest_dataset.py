from datetime import datetime
from pathlib import Path

from pyscicat.client import encode_thumbnail, ScicatClient
from pyscicat.model import Attachment, Datablock, DataFile, Dataset, Ownable, OrigDatablock

# Create a client object. The account used should have the ingestor role in SciCat
scicat = ScicatClient(
    base_url="http://localhost:3000/api/v3", username="ingestor", password="aman"
)
print('scicat', scicat)

# Create an Ownable that will get reused for several other Model objects
ownable = Ownable(ownerGroup="magrathea", accessGroups=["deep_though"])
# thumb_path = Path(__file__).parent.parent / "test/data/SciCatLogo.png"
thumb_path = Path(r'C:\Users\avriza\Desktop\pyscicat\tests\test_pyscicat\data\SciCatLogo.png')


# Create a RawDataset object with settings for your choosing. Notice how
# we pass the `ownable` instance.
dataset = Dataset(
    path="/foo/bar",
    size=42,
    owner="slartibartfast",
    contactEmail="slartibartfast@magrathea.org",
    creationLocation="magrathea",
    creationTime=str(datetime.now()),
    type="raw",
    instrumentId="earth",
    proposalId="deepthought",
    dataFormat="planet",
    principalInvestigator="A. Mouse",
    sourceFolder="/foo/bar",
    scientificMetadata={"a": "field"},
    sampleId="gargleblaster",
    **ownable.dict()
)
dataset_id = scicat.upload_new_dataset(dataset)
print('dataset_id', dataset_id)

# Create Datablock with DataFiles
data_file = DataFile(path="file.h5", size=42)
data_block = OrigDatablock(
    size=42, version='1', datasetId=dataset_id, dataFileList=[data_file], **ownable.dict()
)
print(data_block.__dict__)
scicat.upload_dataset_origdatablock(dataset_id=dataset_id, datablockDto=data_block)

# Create Attachment
attachment = Attachment(
    datasetId=dataset_id,
    thumbnail=encode_thumbnail(thumb_path),
    caption="scattering image",
    **ownable.dict()
)
scicat.upload_attachment(attachment)
