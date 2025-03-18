import os
from general_functions import run_rclone_command
from scicat_uploader import *
from scicat_uploader import ScicatDataSetUploader

ENVMAP = {"production": {
            "scicat_url":"http://localhost:3000/api/v3",
            "scicat_pw_var": "SCICAT_PROD_PW" },
      
      "development": {
            "scicat_url":"https://mf-scicat-testenv.lbl.gov/api/v3/",
            "scicat_pw_var": "SCICAT_TEST_PW"}
      }


def pull_gcloud_data(dsid, storage_bucket, dtype):
    '''
    pull existing json from google cloud and add to Crucible Object
    '''
    x = run_rclone_command(f"{storage_bucket}/{dsid}/{dsid}.json", "./", "copy")
    if x.stderr is not None:
        print(f"from pull_gcloud_data: {x.stderr=}")
    print("[scicat upload] got the data")
    ctd = ScicatDataSetUploader(f"{dsid}.json")
    print("[scicat upload] created scicat ctd uploader object")
    return(ctd)



def upload_to_scicat(sc):
      
    # set up the scicat client
    # scicat_url = envmap[env]['scicat_url']
    run_scicat_client = setup_scicat_client()
    print("[scicat upload] setup client")
    
    # check current status and clean up if needed
    cataloged = sc.check_dataset_catalog_status(run_scicat_client)
    print(len(cataloged))
    if len(cataloged) > 0:
        print(f"{cataloged[0]['pid']=} found while searching for {sc.unique_id=}")
    if sc.delete_from_scicat:
        delete_existing_for_overwrite(cataloged, run_scicat_client)

    
    # send to scicat
    sc.build_ownable_from_patch()
    # sc.find_or_add_proposal(run_scicat_client)
    # print("found proposal")
    sc.find_or_add_instrument(run_scicat_client)   
    print("found instrument")
    
    if sc.overwrite_data or len(cataloged) == 0:
        print("uploading to scicat")
        err = sc.to_scicat(run_scicat_client)
        print(err)
        
        if err is None:
            print("adding files")
            sc.catalog_additional_files(run_scicat_client)
            print("adding thumbnails")
            sc.upload_thumbnail_attachments(run_scicat_client)
            print("done")
        elif '500' in str(err):
            print(f'{sc.unique_id} 500 error during to_scicat')
        # else:
        #     assert err is None

    else:
        print("updating existing")
        err = sc.to_scicat_updateonly(run_scicat_client)
        print(err)
        if '404' in str(err):
            print(f'{sc.unique_id} 404 not found')
        # else:
        #     assert err is None
        print("done")


            
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    # parser.add_argument("--dsid", help="Dataset ID to transfer", required=True)
    # parser.add_argument("--bucket", help="Source bucket", required=True)
    # args = parser.parse_args()

    # Assuming the dataset JSON file is named as "{dsid}.json" and located in the current directory.
    # sc = ScicatDataSetUploader(r"C:\Users\avriza\Desktop\pyscicat\examples\0sqv100jdsyaq000yrkmr8n4ac\0sqv100jdsyaq000yrkmr8n4ac.json")
    # sc = ScicatDataSetUploader(r"C:\Users\avriza\Desktop\pyscicat\examples\test2\0sw0e7emt5vnh000kz4skg3xzw.json")

    # sc =  ScicatDataSetUploader(r"C:\Users\avriza\Desktop\pyscicat\examples\test3\db63sg4p697mh9yhc9mzcyhydw.json")
    sc =  ScicatDataSetUploader(r"C:\Users\avriza\Desktop\pyscicat\examples\test3\test.json")
    run_scicat_client = setup_scicat_client()
    # cataloged = sc.check_dataset_catalog_status(run_scicat_client)
    # print(cataloged)
    # datasets = run_scicat_client.datasets_find()
    # print("🔍 All datasets in SciCat:", datasets)
    # sc = pull_gcloud_data(args.dsid, args.bucket, dtype= "raw")
    upload_to_scicat(sc)
    
    print(f"uploaded to test scicat")
