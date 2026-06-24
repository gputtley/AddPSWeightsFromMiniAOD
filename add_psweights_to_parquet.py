#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import ROOT
import pandas as pd

ROOT.gROOT.SetBatch(True)

ROOT.gSystem.Load("libFWCoreFWLite")
ROOT.gSystem.Load("libDataFormatsFWLite")
ROOT.gSystem.Load("libSimDataFormatsGeneratorProducts")
ROOT.FWLiteEnabler.enable()

parser = argparse.ArgumentParser()
parser.add_argument('--input-file', help='Input file location', type=str, default="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Merged/output/170626_test_parquet_v2/TTToSemiLeptonic_2018.parquet")
parser.add_argument('--director', help='Director JSON file location', type=str, default="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json")
parser.add_argument('--output-folder', help='Output folder location', type=str, default=None)
parser.add_argument('--specific-index', help='Specific file index to process', type=int, default=None)
parser.add_argument('--submit', help='Whether to submit to batch system', action='store_true')
parser.add_argument('--collect', help='Whether to collect batch outputs', action='store_true')
parser.add_argument('--file-index-name', help='Column name for file index in director', type=str, default="EventInfo_file_index")
parser.add_argument('--run-name', help='Column name for run', type=str, default="EventInfo_run")
parser.add_argument('--lumi-block-name', help='Column name for luminosity block', type=str, default="EventInfo_luminosityBlock")
parser.add_argument('--event-name', help='Column name for event', type=str, default="EventInfo_event")
args = parser.parse_args()

# Add inputs
input_file = args.input_file
director = args.director
if args.output_folder is not None:
  output_folder = args.output_folder
else:
  output_folder = "/".join(input_file.split("/")[:-1]) + "_with_psweights"

# Define PS weights
ps_weights_conversion = {
  "nominal" : "central",
  "Baseline": "replica",
  "fsr:murfac=0.707": "fsrRedHi",
  "fsr:murfac=1.414": "fsrRedLo",
  "fsr:murfac=0.5": "fsrDefHi",
  "fsr:murfac=2.0": "fsrDefLo",
  "fsr:murfac=0.25":  "fsrConHi",
  "fsr:murfac=4.0":  "fsrConLo",
  "fsr:g2gg:murfac=0.5":  "fsr_G2GG_muR_dn",
  "fsr:g2gg:murfac=2.0":  "fsr_G2GG_muR_up",
  "fsr:g2qq:murfac=0.5":  "fsr_G2QQ_muR_dn",
  "fsr:g2qq:murfac=2.0":  "fsr_G2QQ_muR_up",
  "fsr:q2qg:murfac=0.5":  "fsr_Q2QG_muR_dn",
  "fsr:q2qg:murfac=2.0":  "fsr_Q2QG_muR_up",
  "fsr:x2xg:murfac=0.5":  "fsr_X2XG_muR_dn",
  "fsr:x2xg:murfac=2.0":  "fsr_X2XG_muR_up",
  "fsr:g2gg:cns=-2.0":  "fsr_G2GG_cNS_dn",
  "fsr:g2gg:cns=2.0":  "fsr_G2GG_cNS_up",
  "fsr:g2qq:cns=-2.0":  "fsr_G2QQ_cNS_dn",
  "fsr:g2qq:cns=2.0":  "fsr_G2QQ_cNS_up",
  "fsr:q2qg:cns=-2.0":  "fsr_Q2QG_cNS_dn",
  "fsr:q2qg:cns=2.0":  "fsr_Q2QG_cNS_up",
  "fsr:x2xg:cns=-2.0":  "fsr_X2XG_cNS_dn",
  "fsr:x2xg:cns=2.0":  "fsr_X2XG_cNS_up",
  "isr:murfac=0.707": "isrRedHi",
  "isr:murfac=1.414": "isrRedLo",
  "isr:murfac=0.5": "isrDefHi",
  "isr:murfac=2.0": "isrDefLo",
  "isr:murfac=0.25": "isrConHi",
  "isr:murfac=4.0": "isrConLo",
  "isr:g2gg:murfac=0.5": "isr_G2GG_muR_dn",
  "isr:g2gg:murfac=2.0": "isr_G2GG_muR_up",
  "isr:g2qq:murfac=0.5": "isr_G2QQ_muR_dn",
  "isr:g2qq:murfac=2.0": "isr_G2QQ_muR_up",
  "isr:q2qg:murfac=0.5": "isr_Q2QG_muR_dn",
  "isr:q2qg:murfac=2.0": "isr_Q2QG_muR_up",
  "isr:x2xg:murfac=0.5": "isr_X2XG_muR_dn",
  "isr:x2xg:murfac=2.0": "isr_X2XG_muR_up",
  "isr:g2gg:cns=-2.0": "isr_G2GG_cNS_dn",
  "isr:g2gg:cns=2.0": "isr_G2GG_cNS_up",
  "isr:g2qq:cns=-2.0": "isr_G2QQ_cNS_dn",
  "isr:g2qq:cns=2.0": "isr_G2QQ_cNS_up",
  "isr:q2qg:cns=-2.0": "isr_Q2QG_cNS_dn",
  "isr:q2qg:cns=2.0": "isr_Q2QG_cNS_up",
  "isr:x2xg:cns=-2.0": "isr_X2XG_cNS_dn",
  "isr:x2xg:cns=2.0": "isr_X2XG_cNS_up",
}


# Get proxy file
proxy = os.getenv("X509_USER_PROXY")
print(f"Using proxy: {proxy}")
if args.submit:
  if proxy is None or proxy == "":
    print("Error: X509_USER_PROXY environment variable not set")
    exit(1)

SITE_REDIRECTORS = {
    "T0_CH_CERN": "root://cms-xrd-global.cern.ch",
    "T1_DE_KIT_Disk": "root://xrootd-cms.infn.it",
    "T2_DE_DESY": "root://xrootd-cms.infn.it",
    "T2_DE_RWTH": "root://xrootd-cms.infn.it",
    "T1_US_FNAL_Disk": "root://cmsxrootd.fnal.gov",
    "T2_US_Florida": "root://cmsxrootd.fnal.gov",
    "T2_US_MIT": "root://cmsxrootd.fnal.gov",
    "T2_US_Nebraska": "root://cmsxrootd.fnal.gov",
    "T2_US_Purdue": "root://cmsxrootd.fnal.gov",
    "T2_US_UCSD": "root://cmsxrootd.fnal.gov",
    "T2_US_Vanderbilt": "root://cmsxrootd.fnal.gov",
    "T3_US_FNALLPC": "root://cmsxrootd.fnal.gov",
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
}

# Load in parquet file
df = pd.read_parquet(input_file)

# Get all unique file_inde
unique_file_indices = df[args.file_index_name].unique()

# Get the dataset name from the parquet file
dataset_name = input_file.split("/")[-1].split(".")[0]
with open(director, "r") as f:
  director_dict = json.load(f)
file_names = []
indices = []
for idx in unique_file_indices:
  if idx == -1: continue
  file_name = director_dict[dataset_name]["files"][idx]
  file_names.append(file_name)
  indices.append(idx)

# Make event key function
def event_key(run, lumi, event):
  return (int(run), int(lumi), int(event))

# Get parent files function
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
      raise ValueError(f"No redirector found for parent file {parent} with sites {out}")
      
  return parents_with_redirector

if not args.collect:
  # Loop over filenames
  matched_rows = []
  matched_indices = []
  for ind, nano_filename in enumerate(file_names):

    if args.specific_index is not None:
      if ind != args.specific_index:
        continue

    if args.submit:
      cmd = f"python3 add_psweights_to_parquet.py --input-file {args.input_file} --director {args.director} --output-folder {output_folder} --specific-index {ind}"
      # make .sh and .sub files 
      job_name = f"jobs/add_psweights_{dataset_name}_{ind}"
      subprocess.run(["mkdir", "-p", "jobs"])
      with open(f"{job_name}.sh", "w") as f:
        f.write("#!/bin/bash\n")
        f.write("export X509_USER_PROXY=$1\n")
        f.write("source /cvmfs/cms.cern.ch/cmsset_default.sh\n")
        file_dir = os.path.dirname(os.path.abspath(__file__))
        f.write(f"cd {file_dir}\n")
        f.write("eval `scramv1 runtime -sh`\n")
        f.write(f"{cmd}\n")
      with open(f"{job_name}.sub", "w") as f:
        f.write(f"Proxy_path = {proxy}\n")
        f.write("arguments = $(Proxy_path)\n")
        f.write(f"executable = {job_name}.sh\n")
        f.write(f"output = {job_name}.out\n")
        f.write(f"error = {job_name}.err\n")
        f.write(f"log = {job_name}.log\n")
        f.write("request_cpus = 4\n")
        f.write(f'+JobFlavour = "longlunch"\n')
        f.write(f'should_transfer_files = YES\n')
        f.write(f'when_to_transfer_output = ON_EXIT\n')
        f.write(f'transfer_output_files = ""\n')
        f.write(f'transfer_input_files = $(Proxy_path)\n')
        f.write("queue 1\n")
      subprocess.run(["condor_submit", f"{job_name}.sub"])
      print(f"Submitted job for {nano_filename} (index {ind})")
      continue

    print(f"Processing {nano_filename} (index {ind}/{len(file_names)})")

    # Skim the parquet file
    df_skim = df[df[args.file_index_name] == indices[ind]]
    nano_keys = set(
        event_key(r, l, e)
        for r, l, e in zip(
            df_skim[args.run_name],
            df_skim[args.lumi_block_name],
            df_skim[args.event_name],
        )
    )
    if len(nano_keys) == 0: continue

    # Need to get the parent of the file name
    parent_files = get_parent_files(nano_filename)

    # Loop through miniaod files
    for mini_ind, fname in enumerate(parent_files):

      print(f"  Processing {fname} (index {mini_ind}/{len(parent_files)})")

      # Open the ROOT file and get the tree
      f = ROOT.TFile.Open(fname)
      t = f.Get("Events")

      # Get the weight names
      lb = f.Get("LuminosityBlocks")
      PS_WEIGHTS = {}
      for i in range(lb.GetEntries()):
        lb.GetEntry(i)

        # try __SIM and if not use __GEN
        if lb.GenLumiInfoHeader_generator__SIM is not None:
          header = lb.GenLumiInfoHeader_generator__SIM
          ending = "SIM"
        else:
          header = lb.GenLumiInfoHeader_generator__GEN
          ending = "GEN"

        weight_names = header.weightNames()
        for j in range(weight_names.size()):
          PS_WEIGHTS[j] = ps_weights_conversion[str(weight_names[j])]
        break

      # Only read the branches you need
      t.SetBranchStatus("*", 0)
      t.SetBranchStatus("EventAuxiliary*", 1)
      t.SetBranchStatus(f"GenEventInfoProduct_generator__{ending}*", 1)

      # Useful for remote XRootD reads
      t.SetCacheSize(50 * 1024 * 1024)
      t.AddBranchToCache("EventAuxiliary", True)
      t.AddBranchToCache(f"GenEventInfoProduct_generator__{ending}.", True)

      # Event loop
      for i in range(t.GetEntries()):

        t.GetEntry(i)
        aux = t.EventAuxiliary
        run = int(aux.run())
        luminosityBlock = int(aux.luminosityBlock())
        event = int(aux.event())

        key = event_key(run, luminosityBlock, event)
        if key not in nano_keys: continue
        matched_index = df_skim.index[
            (df_skim[args.run_name] == run) &
            (df_skim[args.lumi_block_name] == luminosityBlock) &
            (df_skim[args.event_name] == event)
        ]
        matched_indices.append(int(matched_index[0]))

        if ending == "SIM":
          gen = t.GenEventInfoProduct_generator__SIM
        else:
          gen = t.GenEventInfoProduct_generator__GEN

        try:
          weights = gen.weights()
        except ReferenceError:
          print(f"  Warning: null GenEventInfoProduct at MiniAOD entry {i}")
          continue

        nweights = int(weights.size())
        row = {}
        central = float(weights[0]) if nweights > 0 else None
        for idx, name in PS_WEIGHTS.items():
          raw = float(weights[idx]) if idx < nweights else None
          if raw is not None and central not in (None, 0.0):
            row[f"psWeightRel_{name}"] = raw / central
          else:
            row[f"psWeightRel_{name}"] = -999

        matched_rows.append(row)

  if not args.submit:
    # Make dataframe of matched rows and merge with original dataframe
    df_matched = pd.DataFrame(matched_rows, index=matched_indices)
    if args.specific_index is not None:
      df = df[df[args.file_index_name] == indices[args.specific_index]]

    df_final = pd.concat([df, df_matched], axis=1)

    # Save to new parquet file
    output_file = f"{output_folder}/{input_file.split('/')[-1]}"
    if args.specific_index is not None:
      output_file = output_file.replace(".parquet", f"_index_{args.specific_index}.parquet")
    subprocess.run(["mkdir", "-p", output_folder])
    df_final.to_parquet(output_file)
    print(f"Created new parquet file with PS weights: {output_file}")


if args.collect:
  # Collect outputs from batch jobs
  failed_jobs = False
  all_output_files = []
  for ind, nano_filename in enumerate(file_names):
    output_file = f"{output_folder}/{input_file.split('/')[-1]}"
    output_file = output_file.replace(".parquet", f"_index_{ind}.parquet")
    all_output_files.append(output_file)
    if not os.path.exists(f"{output_file}"):
      print(f"Output file for index {ind} not found: {output_file}")
      failed_jobs = True

  if not failed_jobs:
    # Merge all output files into one
    df_list = [pd.read_parquet(f) for f in all_output_files]
    df_final = pd.concat(df_list, axis=0)
    final_output_file = f"{output_folder}/{input_file.split('/')[-1]}"
    df_final.to_parquet(final_output_file)
    print(f"Created final parquet file with PS weights: {final_output_file}")