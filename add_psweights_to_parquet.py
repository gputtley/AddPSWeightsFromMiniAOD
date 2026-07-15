#!/usr/bin/env python3

import argparse
import json
import os
import subprocess
import ROOT
import time
import pandas as pd
import pyarrow.parquet as pq
import numpy as np

from concurrent.futures import ThreadPoolExecutor, as_completed

from functions import (
  event_key,
  get_parent_files,
  make_and_submit_batch_job,
  run_das_query,
  strip_redirector,
  get_child_parent_dataset_dictionary,
  find_redirector_for_file,
)

ROOT.gROOT.SetBatch(True)

ROOT.gSystem.Load("libFWCoreFWLite")
ROOT.gSystem.Load("libDataFormatsFWLite")
ROOT.gSystem.Load("libSimDataFormatsGeneratorProducts")
ROOT.FWLiteEnabler.enable()

parser = argparse.ArgumentParser()
parser.add_argument('--input-folder', help='Input folder location', type=str, default="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Merged/output/170626_test_parquet_v2")
parser.add_argument('--input-file', help='Input file location, this can be a comma-separated list of files', type=str, default="TTToSemiLeptonic_2018")
parser.add_argument('--director', help='Director JSON file location', type=str, default="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json")
parser.add_argument('--output-folder', help='Output folder location', type=str, default=None)
parser.add_argument('--specific-batch-index', help='Specific batch index to process', type=int, default=None)
parser.add_argument('--specific-nano-index', help='Specific NanoAOD file index to process', type=int, default=None)
parser.add_argument('--submit', help='Whether to submit to batch system', action='store_true')
parser.add_argument('--collect', help='Whether to collect batch outputs', action='store_true')
parser.add_argument('--file-index-name', help='Column name for file index in director', type=str, default="EventInfo_file_index")
parser.add_argument('--run-name', help='Column name for run', type=str, default="EventInfo_run")
parser.add_argument('--lumi-block-name', help='Column name for luminosity block', type=str, default="EventInfo_luminosityBlock")
parser.add_argument('--event-name', help='Column name for event', type=str, default="EventInfo_event")
parser.add_argument('--batch-size', help='Batch size for processing parquet files', type=int, default=500000)
parser.add_argument('--dry-run', help='Whether to perform a dry run', action='store_true')
args = parser.parse_args()

# Define input files
if "," in args.input_file:
  initial_input_files = [f"{args.input_folder}/{f}.parquet" for f in args.input_file.split(",")]
else:
  initial_input_files = [f"{args.input_folder}/{args.input_file}.parquet"]
input_files = []
for f in initial_input_files:
  if os.path.exists(f):
    input_files.append(f)
  else:
    print(f"Input file {f} does not exist. Skipping.")

# Define director file
if "," in args.director:
  directors = [f for f in args.director.split(",")]
else:
  directors = [args.director]
director_dict = {}
for director in directors:
  with open(director, "r") as f:
    director_dict.update(json.load(f))

# Define output folder
if args.output_folder is not None:
  output_folder = args.output_folder
else:
  if args.input_folder.endswith("/"):
    output_folder = args.input_folder[:-1] + "_with_psweights"
  else:
    output_folder = args.input_folder + "_with_psweights" 

# Check if specific_nano_index is set and no specific_batch_index is set, then raise an error
if args.specific_nano_index is not None and args.specific_batch_index is None:
  print("Error: --specific-nano-index is set but --specific-batch-index is not set. Please set --specific-batch-index to the batch index containing the specific NanoAOD file.")
  exit(1)

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
  "fsr.murfac=0.707": "fsrRedHi",
  "fsr.murfac=1.414": "fsrRedLo",
  "fsr.murfac=0.5": "fsrDefHi",
  "fsr.murfac=2.0": "fsrDefLo",
  "fsr.murfac=0.25":  "fsrConHi",
  "fsr.murfac=4.0":  "fsrConLo",
  "fsr.g2gg.murfac=0.5":  "fsr_G2GG_muR_dn",
  "fsr.g2gg.murfac=2.0":  "fsr_G2GG_muR_up",
  "fsr.g2qq.murfac=0.5":  "fsr_G2QQ_muR_dn",
  "fsr.g2qq.murfac=2.0":  "fsr_G2QQ_muR_up",
  "fsr.q2qg.murfac=0.5":  "fsr_Q2QG_muR_dn",
  "fsr.q2qg.murfac=2.0":  "fsr_Q2QG_muR_up",
  "fsr.x2xg.murfac=0.5":  "fsr_X2XG_muR_dn",
  "fsr.x2xg.murfac=2.0":  "fsr_X2XG_muR_up",
  "fsr.g2gg.cns=-2.0":  "fsr_G2GG_cNS_dn",
  "fsr.g2gg.cns=2.0":  "fsr_G2GG_cNS_up",
  "fsr.g2qq.cns=-2.0":  "fsr_G2QQ_cNS_dn",
  "fsr.g2qq.cns=2.0":  "fsr_G2QQ_cNS_up",
  "fsr.q2qg.cns=-2.0":  "fsr_Q2QG_cNS_dn",
  "fsr.q2qg.cns=2.0":  "fsr_Q2QG_cNS_up",
  "fsr.x2xg.cns=-2.0":  "fsr_X2XG_cNS_dn",
  "fsr.x2xg.cns=2.0":  "fsr_X2XG_cNS_up",
  "isr.murfac=0.707": "isrRedHi",
  "isr.murfac=1.414": "isrRedLo",
  "isr.murfac=0.5": "isrDefHi",
  "isr.murfac=2.0": "isrDefLo",
  "isr.murfac=0.25": "isrConHi",
  "isr.murfac=4.0": "isrConLo",
  "isr.g2gg.murfac=0.5": "isr_G2GG_muR_dn",
  "isr.g2gg.murfac=2.0": "isr_G2GG_muR_up",
  "isr.g2qq.murfac=0.5": "isr_G2QQ_muR_dn",
  "isr.g2qq.murfac=2.0": "isr_G2QQ_muR_up",
  "isr.q2qg.murfac=0.5": "isr_Q2QG_muR_dn",
  "isr.q2qg.murfac=2.0": "isr_Q2QG_muR_up",
  "isr.x2xg.murfac=0.5": "isr_X2XG_muR_dn",
  "isr.x2xg.murfac=2.0": "isr_X2XG_muR_up",
  "isr.g2gg.cns=-2.0": "isr_G2GG_cNS_dn",
  "isr.g2gg.cns=2.0": "isr_G2GG_cNS_up",
  "isr.g2qq.cns=-2.0": "isr_G2QQ_cNS_dn",
  "isr.g2qq.cns=2.0": "isr_G2QQ_cNS_up",
  "isr.q2qg.cns=-2.0": "isr_Q2QG_cNS_dn",
  "isr.q2qg.cns=2.0": "isr_Q2QG_cNS_up",
  "isr.x2xg.cns=-2.0": "isr_X2XG_cNS_dn",
  "isr.x2xg.cns=2.0": "isr_X2XG_cNS_up",
}

# Get proxy file
proxy = os.getenv("X509_USER_PROXY")
print(f"Using proxy: {proxy}")
if args.submit:
  if proxy is None or proxy == "":
    print("Error: X509_USER_PROXY environment variable not set")
    exit(1)

# Loop over input files
for file_ind, input_file in enumerate(input_files):

  print(f"- {file_ind+1}/{len(input_files)}: Processing input file {input_file}")

  written_files = []

  ## Load in parquet file
  metadata = pq.read_metadata(input_file)
  n_rows = metadata.num_rows
  n_batches = int((n_rows // args.batch_size) + 1)
  parquet_file = pq.ParquetFile(input_file)
  for batch_ind, batch in enumerate(parquet_file.iter_batches(batch_size=args.batch_size)):

    df = batch.to_pandas()

    if args.specific_batch_index is not None:
      if batch_ind != args.specific_batch_index:
        continue

    print(f"  = {batch_ind+1}/{n_batches}: Processing batch {batch_ind+1} of {n_batches} with {len(df)} rows")

    # Get all unique file_inde
    unique_file_indices = df[args.file_index_name].unique()

    # Get the dataset name from the parquet file
    dataset_name = input_file.split("/")[-1].split(".")[0]
    file_names = []
    indices = []
    for idx in unique_file_indices:
      if idx == -1: continue
      file_name = director_dict[dataset_name]["files"][idx]
      file_names.append(file_name)
      indices.append(idx)


    # Get the weights
    if not args.collect:

      # Add written_files to list
      for ind, nano_filename in enumerate(file_names):
        if args.specific_nano_index is not None:
          if ind != args.specific_nano_index:
            continue
        output_file = f"{output_folder}/{input_file.split('/')[-1]}"
        output_file = output_file.replace(".parquet", f"_batch_index_{batch_ind}.parquet")
        if args.submit or args.specific_nano_index is not None:
          output_file = output_file.replace(".parquet", f"_nano_index_{ind}.parquet")
        written_files.append(output_file)

      # Submit
      if args.submit:
        make_and_submit_batch_job(dataset_name, args, input_file.split(".parquet")[0], output_folder, proxy, file_names, batch_ind, dry_run=args.dry_run)

      else:

        # Get the child-parent-dataset dictionary
        child_parent_dataset_dict = get_child_parent_dataset_dictionary(file_names)

        # Loop over filenames
        matched_rows = []
        matched_indices = []
        for ind, nano_filename in enumerate(file_names):

          if args.specific_nano_index is not None:
            if ind != args.specific_nano_index:
              continue

          total_matched_in_nano_file = 0

          print(f"    * {ind+1}/{len(file_names)}: Processing NanoAOD file {nano_filename}")

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
          parent_files_no_redirector = child_parent_dataset_dict[strip_redirector(nano_filename)]["miniaod_files"]
          parent_files = []
          for f in parent_files_no_redirector:
            redirector = find_redirector_for_file(f)
            if redirector is not None:
              parent_files.append(f"{redirector}/{f}")
            else:
              print(f"WARNING: No redirector found for {f}. Skipping file.")
              ## Try aod files
              #aod_parent_files = get_parent_files(f)
              #if len(aod_parent_files) > 0:
              #  parent_files.extend(aod_parent_files)
              #else:
              #  print(f"WARNING: No site found for {f} of its parent. Skipping file.")

          # Loop through miniaod files
          for mini_ind, fname in enumerate(parent_files):

            print(f"      + {mini_ind+1}/{len(parent_files)}: Processing parent file {fname}")

            # Open the ROOT file and get the tree
            f = ROOT.TFile.Open(fname ,"READ")
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
              total_matched_in_nano_file += 1

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
              row = {"matched_to_mini": 1}
              central = float(weights[0]) if nweights > 0 else None
              for idx, name in PS_WEIGHTS.items():
                raw = float(weights[idx]) if idx < nweights else None
                if raw is not None and central not in (None, 0.0):
                  row[f"psWeightRel_{name}"] = raw / central
                else:
                  row[f"psWeightRel_{name}"] = -999

              matched_rows.append(row)

          print(f"    Found {total_matched_in_nano_file} matched rows out of {len(nano_keys)} events in the NanAOD file")


        # Make dataframe of matched rows and merge with original dataframe
        df_matched = pd.DataFrame(matched_rows, index=matched_indices)
        if args.specific_nano_index is not None:
          df = df[df[args.file_index_name] == indices[args.specific_nano_index]]
        
        df_final = pd.concat([df, df_matched], axis=1)

        # Fill in empty rows for events that were not matched
        df_final["matched_to_mini"] = df_final["matched_to_mini"].fillna(0)
        df_final = df_final.fillna(-999)

        # Save to new parquet file
        output_file = f"{output_folder}/{input_file.split('/')[-1]}"
        output_file = output_file.replace(".parquet", f"_batch_index_{batch_ind}.parquet")
        if args.specific_nano_index is not None:
          output_file = output_file.replace(".parquet", f"_nano_index_{args.specific_nano_index}.parquet")
        subprocess.run(["mkdir", "-p", output_folder])
        df_final.to_parquet(output_file)
        print(f"  Created new parquet file with PS weights: {output_file}")


  # Collect batch outputs if not specific_nano_index
  if not args.submit and not args.collect and args.specific_nano_index is None:
    df_list = [pd.read_parquet(f) for f in written_files]
    df_final = pd.concat(df_list, axis=0)
    final_output_file = f"{output_folder}/{input_file.split('/')[-1]}"
    df_final.to_parquet(final_output_file)
    print(f"  Created final parquet file with PS weights: {final_output_file}")

  # Write a json file with the list of written files
  if args.submit:
    written_files_json = f"{output_folder}/{input_file.replace('.parquet', '').split('/')[-1]}_written_files.json"
    subprocess.run(["mkdir", "-p", output_folder])
    with open(written_files_json, "w") as f:
      json.dump(written_files, f, indent=2)
    print(f"  Wrote list of written files to: {written_files_json}")

  # Collect batch jobs
  if args.collect:
    # Collect outputs from batch jobs
    failed_jobs = False
    all_output_files = []
    for ind, nano_filename in enumerate(file_names):
      output_file = f"{output_folder}/{input_file.split('/')[-1]}"
      output_file = output_file.replace(".parquet", f"_index_{ind}.parquet")
      all_output_files.append(output_file)
      if not os.path.exists(f"{output_file}"):
        print(f"  Output file for index {ind} not found: {output_file}")
        failed_jobs = True

    if not failed_jobs:
      # Merge all output files into one
      df_list = [pd.read_parquet(f) for f in all_output_files]
      df_final = pd.concat(df_list, axis=0)
      final_output_file = f"{output_folder}/{input_file.split('/')[-1]}"
      df_final.to_parquet(final_output_file)
      print(f"  Created final parquet file with PS weights: {final_output_file}")