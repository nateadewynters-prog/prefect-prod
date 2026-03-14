import os
import pytest
from src.env_setup import setup_environment
from src.sharepoint_uploader import SharePointUploader

def test_live_sharepoint_upload():
    """
    LIVE INTEGRATION TEST: Verifies uploading a dummy file to the SALES REPORTING SharePoint site.
    """
    setup_environment()

    print("\n🚀 Initializing SharePoint Uploader...")
    try:
        uploader = SharePointUploader()
    except ValueError as e:
        pytest.fail(f"❌ Initialization failed. Check your .env variables: {e}")

    # 1. Create a tiny dummy file locally
    dummy_filename = "dummy_upload_test.txt"
    dummy_filepath = os.path.join(os.getcwd(), dummy_filename)

    with open(dummy_filepath, "w") as f:
        f.write("Hello from the DataOps Python pipeline! This is an automated upload test.")

    print(f"📄 Created temporary dummy file: {dummy_filepath}")

    # 2. Define our test destination (using your existing Automation QA folder)
    show_name = "Automation QA"
    venue_name = "Upload_Test"
    folder_type = "Raw"

    print(f"☁️ Attempting upload to: {show_name}/{venue_name}/{folder_type}/{dummy_filename} ...")

    try:
        # 3. Trigger the upload
        success = uploader.upload_file(
            local_file_path=dummy_filepath,
            filename=dummy_filename,
            show_name=show_name,
            venue_name=venue_name,
            folder_type=folder_type
        )
        
        # 4. Verify it worked
        assert success is True, "❌ The uploader returned False. Check the logs above for the Microsoft Graph API error."
        print("\n🎉 SUCCESS! The dummy file was successfully pushed to SharePoint.")
        
    finally:
        # 5. Clean up the local file no matter what happens
        if os.path.exists(dummy_filepath):
            os.remove(dummy_filepath)
            print("🧹 Cleaned up local dummy file.")

if __name__ == "__main__":
    test_live_sharepoint_upload()
