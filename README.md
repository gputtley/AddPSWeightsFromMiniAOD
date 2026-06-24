# AddPSWeightsFromMiniAOD

This is a repository to add the FSR and ISR decorrelated PS weights from MiniAOD to skims produced from NanoAOD. 

Currently the script is set up to input and output a parquet file for these skims. The code works on the principal that the specific NanoAOD ROOT file is accessible from an index store in the column of the parquet file and an accomponying json director file, where the keys are the file_name (without the directory or the extension), then "files", and the values are the list of NanoAOD where the index refers to. Also required is the run, event and luminosity block of the particular event, in order to match to MiniAOD.

The code takes in a single parquet file at a time, loop over the NanoAOD ROOT files within the dataset, and then MiniAOD parents of that file, in order to find the matching event. It is set up so that it can be parallelised and submit to the CERN htcondor cluster for every NanoAOD ROOT file.

The outputs of the code will be a parquet containing all previous columns and the MiniAOD PS weights, with the column names starting with 'psWeightRel_' and ending in a clear label for what it represents. These are the weights relative to the central weight.

## Setting up the code

The code is set up to work in the environment of `CMSSW_14_0_23`. CMSSW is not used within the code but only use as an environment. It can be setup with the following commands.

```
source /cvmfs/cms.cern.ch/cmsset_default.sh
cmsrel CMSSW_14_0_23
cd CMSSW_14_0_23/src
cmsenv
```

## Setting grid proxy

As you will need to open MiniAOD files, you will need to setup your grid proxy. As the code later supports batch submission, it is recommended to set up with the following script.

```
source proxy_setup.sh
```

## Running the code

The code is ran through the script `add_psweights_to_parquet.py`, which comes with a number of options:

* `--input-file`: The full path to the parquet file you wish to add the weights for.
* `--director`: The full path to the json director containing the NanoAOD ROOT file names.
* `--output-folder`: The folder you wish the code to write the output. If unset will you the directory of the input-file but with a `_with_psweight` extension.
* `--specific-index`: If set (with an integer input), it will only run this specific NanoAOD ROOT file index.
* `--submit`: If set (to true), it will submit the jobs to the CERN htcondor cluster.
* `--collect`: If set (to true), it will collect the outputs of the jobs submitted, if they all exist.
* `--file-index-name`: The column name of the NanoAOD ROOT file index.
* `--run-name`: The column name of the run number.
* `--lumi-block-name`: The column name of the luminosity block.
* `--event-name`: The column name of the event number

### Running a test

To run a test on a single NanoAOD ROOT file, you can use the command:

```
python3 add_psweights_to_parquet.py --input-file="/eos/user/g/guttley/pc_output/230626_btag_parquet/TTToSemiLeptonic_2023_postBPix.parquet" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json" --specific-index=0
```

## Running locally (slow)

To run the whole parquet file locally, you can use the command:

```
python3 add_psweights_to_parquet.py --input-file="/eos/user/g/guttley/pc_output/230626_btag_parquet/TTToSemiLeptonic_2023_postBPix.parquet" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json"
```

## Running on the batch

To submit the jobs to the CERN htcondor cluster, you can use the command:

```
python3 add_psweights_to_parquet.py --input-file="/eos/user/g/guttley/pc_output/230626_btag_parquet/TTToSemiLeptonic_2023_postBPix.parquet" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json" --submit
```

Once all the jobs have finished you can collect the outputs with the command:

```
python3 add_psweights_to_parquet.py --input-file="/eos/user/g/guttley/pc_output/230626_btag_parquet/TTToSemiLeptonic_2023_postBPix.parquet" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json" --collect
```