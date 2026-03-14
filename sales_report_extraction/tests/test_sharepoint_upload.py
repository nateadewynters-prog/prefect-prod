import os
import pytest
from src.env_setup import setup_environment
from src.sharepoint_uploader import SharePointUploader

def test_live_sharepoint_upload():
    """LIVE INTEGRATION TEST: Verifies uploading a dummy file to SharePoint."""
    setup_environment()

    print("\n🚀 Initializing SharePoint Uploader...")
    try:
        uploader = SharePointUploader()
    except ValueError as e:
        pytest.fail(f"❌ Initialization failed. Check your .env variables: {e}")

    dummy_filename = "dummy_upload_test.txt"
    dummy_filepath = os.path.join(os.getcwd(), dummy_filename)

    with open(dummy_filepath, "w") as f:
        f.write("Hello from the DataOps Python pipeline! This is an automated upload test.")

    print(f"📄 Created temporary dummy file: {dummy_filepath}")

    show_name = "Automation QA"
    venue_name = "Upload_Test"
    folder_type = "Raw"

    print(f"☁️ Attempting upload to: {show_name}/{venue_name}/{folder_type}/{dummy_filename} ...")

    try:
        # 🚀 FIX: We now expect a URL string back, not 'True'
        result_url = uploader.upload_file(
            local_file_path=dummy_filepath,
            filename=dummy_filename,
            show_name=show_name,
            venue_name=venue_name,
            folder_type=folder_type
        )
        
        # 🚀 FIX: Assert we got a string URL back
        assert isinstance(result_url, str) and result_url.startswith("http"), "❌ Uploader did not return a valid URL."
        print(f"\n🎉 SUCCESS! The dummy file was successfully pushed to SharePoint.\n🔗 Link: {result_url}")
        
    finally:
        if os.path.exists(dummy_filepath):
            os.remove(dummy_filepath)
            print("🧹 Cleaned up local dummy file.")

if __name__ == "__main__":
    test_live_sharepoint_upload()
