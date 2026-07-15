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
* `--specific-nano-index`: If set (with an integer input), it will only run this specific NanoAOD ROOT file index.
* `--submit`: If set (to true), it will submit the jobs to the CERN htcondor cluster.
* `--collect`: If set (to true), it will collect the outputs of the jobs submitted, if they all exist.
* `--file-index-name`: The column name of the NanoAOD ROOT file index.
* `--run-name`: The column name of the run number.
* `--lumi-block-name`: The column name of the luminosity block.
* `--event-name`: The column name of the event number

### Running a test

To run a test on a single NanoAOD ROOT file, you can use the command:

```
python3 add_psweights_to_parquet.py --input-folder="/eos/user/g/guttley/pc_output/230626_btag_parquet" --input-file="TTToSemiLeptonic_2023_postBPix" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json" --specific-nano-index=0 --specific-batch-index=0
```

## Running locally (slow)

To run the whole parquet file locally, you can use the command:

```
python3 add_psweights_to_parquet.py --input-folder="/eos/user/g/guttley/pc_output/230626_btag_parquet" --input-file="TTToSemiLeptonic_2023_postBPix" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json"
```

## Running on the batch

To submit the jobs to the CERN htcondor cluster, you can use the command:

```
python3 add_psweights_to_parquet.py --input-folder="/eos/user/g/guttley/pc_output/230626_btag_parquet" --input-file="TTToSemiLeptonic_2023_postBPix" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json" --submit
```

Once all the jobs have finished you can collect the outputs with the command:

```
python3 add_psweights_to_parquet.py --input-folder="/eos/user/g/guttley/pc_output/230626_btag_parquet" --input-file="TTToSemiLeptonic_2023_postBPix" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json" --collect
```

## Running more than one files

You can also run on more than one input parquet file at a time. This can be done by providing a comma separated list for the `--input-file` option. You can also parse a comma separated list for the `--director` option. A full command used for TOP-26-010 is:

```
years=("2016_PreVFP" "2016_PostVFP" "2017" "2018" "2022_preEE" "2022_postEE" "2023_preBPix" "2023_postBPix" "2024")
samples=(
  "TTToSemiLeptonic"
  "TTTo2L2Nu"
  "TTToHadronic"
  "TTMtt700To1000"
  "TTMtt1000"
  "TTToSemiLeptonic166p5"
  "TTToSemiLeptonic169p5"
  "TTToSemiLeptonic171p5"
  "TTToSemiLeptonic173p5"
  "TTToSemiLeptonic175p5"
  "TTToSemiLeptonic178p5"
  "TTTo2L2Nu166p5"
  "TTTo2L2Nu169p5"
  "TTTo2L2Nu171p5"
  "TTTo2L2Nu173p5"
  "TTTo2L2Nu175p5"
  "TTTo2L2Nu178p5"
  "TTToHadronic166p5"
  "TTToHadronic169p5"
  "TTToHadronic171p5"
  "TTToHadronic173p5"
  "TTToHadronic175p5"
  "TTToHadronic178p5"
  "TTToSemiLeptonic_CR1"
  "TTToSemiLeptonic_CR2"
  "TTToSemiLeptonic_hdamp_Up"
  "TTToSemiLeptonic_hdamp_Down"
  "TTToSemiLeptonic_ue_Up"
  "TTToSemiLeptonic_ue_Down"
  "TTToSemiLeptonic_ERDOn"
  "ST_t_channel_top"
  "ST_t_channel_antitop"
  "ST_t_channel_top_hadronic"
  "ST_t_channel_antitop_hadronic"
  "ST_t_channel_top_leptonic"
  "ST_t_channel_antitop_leptonic"
  "ST_s_channel"
  "ST_s_channel_hadronic"
  "ST_s_channel_leptonic"
  "ST_s_channel_top"
  "ST_s_channel_antitop"
  "ST_s_channel_top_leptonic"
  "ST_s_channel_antitop_leptonic"
  "ST_tW_antitop"
  "ST_tW_top"
)
file_string=""
for year in "${years[@]}"; do for sample in "${samples[@]}"; do file_string+="${sample}_${year},"; done; done
file_string="${file_string%,}"
```

```
python3 add_psweights_to_parquet.py --input-folder="/eos/user/g/guttley/pc_output/230626_btag_parquet" --input-file="${file_string}" --director="/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/signals_MC_ttbar.json,/afs/cern.ch/work/g/guttley/private/top_reco/AnalysisConfigs/Datasets/backgrounds_MC_ttbar.json"
```