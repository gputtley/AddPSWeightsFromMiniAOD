import os
import json
import subprocess

from concurrent.futures import ThreadPoolExecutor, as_completed

SITE_REDIRECTORS = {
  "T0_CH_CERN": "root://cms-xrd-global.cern.ch",
  "T2_CH_CERN": "root://cms-xrd-global.cern.ch",
  "T1_DE_KIT_Disk": "root://xrootd-cms.infn.it",
  "T2_DE_DESY": "root://xrootd-cms.infn.it",
  "T2_DE_RWTH": "root://xrootd-cms.infn.it",
  "T1_FR_CCIN2P3_Disk": "root://xrootd-cms.infn.it",
  "T2_FR_GRIF": "root://xrootd-cms.infn.it",
  "T2_FR_IPHC": "root://xrootd-cms.infn.it",
  "T2_UK_London_Brunel": "root://xrootd-cms.infn.it",
  "T2_UK_London_IC": "root://xrootd-cms.infn.it",
  "T2_UK_SGrid_Bristol": "root://xrootd-cms.infn.it",
  "T2_IT_Bari": "root://xrootd-cms.infn.it",
  "T2_IT_Legnaro": "root://xrootd-cms.infn.it",
  "T2_IT_Pisa": "root://xrootd-cms.infn.it",
  "T2_IT_Rome": "root://xrootd-cms.infn.it",
  "T2_ES_CIEMAT": "root://xrootd-cms.infn.it",
  "T2_ES_IFCA": "root://xrootd-cms.infn.it",
  "T2_BE_IIHE": "root://xrootd-cms.infn.it",
  "T2_BE_UCL": "root://xrootd-cms.infn.it",
  "T1_US_FNAL_Disk": "root://cmsxrootd.fnal.gov",
  "T2_US_Florida": "root://cmsxrootd.fnal.gov",
  "T2_US_MIT": "root://cmsxrootd.fnal.gov",
  "T2_US_Nebraska": "root://cmsxrootd.fnal.gov",
  "T2_US_Purdue": "root://cmsxrootd.fnal.gov",
  "T2_US_UCSD": "root://cmsxrootd.fnal.gov",
  "T2_US_Vanderbilt": "root://cmsxrootd.fnal.gov",
  "T3_US_FNALLPC": "root://cmsxrootd.fnal.gov",
}

def event_key(run, lumi, event):
  return (int(run), int(lumi), int(event))


"""
def get_parent_files(nano_file):
  nano_file_no_director = "/store" + nano_file.split("//store")[-1]
  director = nano_file.split("//store")[0]

  query = f"parent file={nano_file_no_director}"
  cmd = [
      "dasgoclient",
      "--query", query,
  ]
  out = subprocess.check_output(
    cmd, 
    text=True,
  )

  parents = [
      line.strip()
      for line in out.splitlines()
      if line.strip()
  ]

  parents_with_redirector = []
  for parent in parents:
    cmd = [
        "dasgoclient",
        "--query", f"site file={parent}",
    ]
    out = subprocess.check_output(
      cmd, 
      text=True,
    )

    # Take the first site in SITE_REDIRECTORS that matches the output    
    redirector = None
    for site, redir in SITE_REDIRECTORS.items():
      if site in out:
        redirector = redir
        break
    if redirector is not None:
      parents_with_redirector.append(f"{redirector}/{parent}")
    else:
      print(f"No redirector found for parent file {parent} with sites {out}. Assuming not a valid MiniAOD file.")
      
  return parents_with_redirector
"""

def make_and_submit_batch_job(dataset_name, args, input_file, output_folder, proxy, file_names, specific_batch_index, dry_run=False):

  out_name = f"add_psweights_{dataset_name}_batch_index_{specific_batch_index}"
  job_name = f"jobs/{out_name}"
  log_name = f"jobs_log/{out_name}"
  err_file_name = f"jobs_err/{out_name}"
  out_file_name = f"jobs_out/{out_name}"
  subprocess.run(["mkdir", "-p", "jobs"])
  subprocess.run(["mkdir", "-p", "jobs_log"])
  subprocess.run(["mkdir", "-p", "jobs_err"])
  subprocess.run(["mkdir", "-p", "jobs_out"])
  with open(f"{job_name}.sh", "w") as f:
    f.write("#!/bin/bash\n")
    f.write("export X509_USER_PROXY=$1\n")
    f.write("source /cvmfs/cms.cern.ch/cmsset_default.sh\n")
    file_dir = os.path.dirname(os.path.abspath(__file__))
    f.write(f"cd {file_dir}\n")
    f.write("eval `scramv1 runtime -sh`\n")
    cmd = f"python3 add_psweights_to_parquet.py --input-file {input_file} --input-folder='' --director {args.director} --output-folder {output_folder} --specific-nano-index $2 --specific-batch-index {specific_batch_index}"
    f.write(f"{cmd}\n")
  with open(f"{job_name}.sub", "w") as f:
    f.write(f"Proxy_path = {proxy}\n")
    f.write("arguments = $(Proxy_path) $(ProcId)\n")
    f.write(f"executable = {job_name}.sh\n")
    f.write(f"output = {out_file_name}_$(ClusterId)_$(ProcId).out\n")
    f.write(f"error = {err_file_name}_$(ClusterId)_$(ProcId).err\n")
    f.write(f"log = {log_name}_$(ClusterId).log\n")
    f.write(f"request_cpus = 4\n")
    f.write(f'+JobFlavour = "longlunch"\n')
    f.write(f'should_transfer_files = YES\n')
    f.write(f'when_to_transfer_output = ON_EXIT\n')
    f.write(f'transfer_output_files = ""\n')
    f.write(f'transfer_input_files = $(Proxy_path)\n')
    f.write(f"queue {len(file_names)}\n")
  if not dry_run:
    subprocess.run(["condor_submit", f"{job_name}.sub"])
    print(f"Submitted job for {dataset_name} with {len(file_names)} NanoAOD files")


def run_das_query(query):
  """Run a DAS query and return non-empty output lines."""
  cmd = ["dasgoclient", "--query", query]
  out = subprocess.check_output(cmd, text=True)
  return [line.strip() for line in out.splitlines() if line.strip()]


def run_das_json_query(query):
  """Run a DAS JSON query and return parsed JSON."""
  cmd = ["dasgoclient", "--json", "--query", query]
  out = subprocess.check_output(cmd, text=True)
  if not out.strip():
    return []
  return json.loads(out)


def strip_redirector(filename):
  if "//store" in filename:
    return "/store" + filename.split("//store", 1)[-1]
  if "/store" in filename:
    return "/store" + filename.split("/store", 1)[-1]
  return filename


def find_redirector_for_file(lfn):
  sites = "\n".join(run_das_query(f"site file={lfn}"))
  for site, redir in SITE_REDIRECTORS.items():
    if site in sites:
      return redir
  return None


def is_valid_file(lfn):
  records = run_das_json_query(f"file file={lfn}")
  for record in records:
    for file_info in record.get("file", []):
      if file_info.get("name") == lfn:
        return file_info.get("is_file_valid") == 1
  return False


def get_valid_children_of_parents(miniaod_file):

  candidates_with_redirector = []

  # Parent(s) of the MiniAOD, usually AODSIM
  grandparent_files = run_das_query(f"parent file={miniaod_file}")


  for grandparent in grandparent_files:

    # Children of the AODSIM parent
    child_files = run_das_query(f"child file={grandparent}")

    for child in child_files:

      # Keep MiniAOD-like children only
      if "/MINIAODSIM/" not in child:
        continue

      # Skip the original bad/unavailable file
      if child == miniaod_file:
        continue

      ## Require DBS-valid file
      #if not is_valid_file(child):
      #  continue

      redirector = find_redirector_for_file(child)
      if redirector is None:
        print(
            f"No redirector found for valid child file {child}. "
            "Skipping."
        )
        continue

        candidates_with_redirector.append(f"{redirector}/{child}")

  # Deduplicate while preserving order
  return list(dict.fromkeys(candidates_with_redirector))


# Get parent files function
def get_parent_files(nano_file):

  nano_file_no_director = strip_redirector(nano_file)
  parents = run_das_query(f"parent file={nano_file_no_director}")

  #print(f"Initially found {len(parents)} MiniAOD parent files")

  parents_with_redirector = []

  for parent in parents:
    redirector = find_redirector_for_file(parent)

    if redirector is not None:
      parents_with_redirector.append(f"{redirector}/{parent}")
      continue

  # Deduplicate while preserving order
  return list(dict.fromkeys(parents_with_redirector))


def get_child_parent_dataset_dictionary(file_names, max_workers=16):

  # Strip redirectors once and avoid duplicate DAS queries
  striped_nano_filenames = sorted(set(strip_redirector(f) for f in file_names))

  results = {
      nano_filename: {
          "miniaod_files": [],
          "datasets": [],
      }
      for nano_filename in striped_nano_filenames
  }

  with ThreadPoolExecutor(max_workers=max_workers) as executor:
      parent_futures = {
          executor.submit(run_das_query, f"parent file={nano_filename}"): nano_filename
          for nano_filename in striped_nano_filenames
      }

      dataset_futures = {
          executor.submit(run_das_query, f"dataset file={nano_filename}"): nano_filename
          for nano_filename in striped_nano_filenames
      }

      for ind, future in enumerate(as_completed(parent_futures)):
          nano_filename = parent_futures[future]
          try:
              results[nano_filename]["miniaod_files"] = sorted(set(future.result()))
          except Exception as e:
              print(f"Failed parent query for {nano_filename}: {e}")
              results[nano_filename]["miniaod_files"] = []

      for ind, future in enumerate(as_completed(dataset_futures)):
          nano_filename = dataset_futures[future]
          try:
              results[nano_filename]["datasets"] = sorted(set(future.result()))
          except Exception as e:
              print(f"Failed dataset query for {nano_filename}: {e}")
              results[nano_filename]["datasets"] = []

  return results
