import os


process_instance_id = 9
process_instance_dir_path = os.path.join("downloaded_docs", "process-instance-" + str(process_instance_id))
BLUEPRINT_PATH = os.path.join(process_instance_dir_path, "blueprint.json")
RESPONSE_BODY_PATH = os.path.join(process_instance_dir_path, "response_body.json")

print("Blueprint PATH > ", BLUEPRINT_PATH)
print("Response Body PATH > ", RESPONSE_BODY_PATH)