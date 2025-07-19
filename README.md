# Metis

Unified and Interpretable Troubleshooting for Microservice Systems using Multi-modal Data


Metis is a unified framework for microservice troubleshooting that integrates Anomaly Detection (AD), Root Cause Localization (RCL), and Fault Type Identification (FTI) into a single end-to-end pipeline.
Unlike existing methods that rely on single-modal signals or disconnected modules, Metis systematically incorporates multi-modal observability data — logs, metrics, and traces — throughout the entire troubleshooting process. It also provides interpretable outputs by linking predictions directly to raw observability data.



## 📦 Module Overview
🔸 AD (Anomaly Detection)
Detects abnormal patterns using:
* log/log_ad.py: log-based detection
* metric/metric_ad.py: metric-based detection
* trace/trace_ad.py: trace-based detection

🔸 RCL (Root Cause Localization)
Pinpoints root cause services based on abnormal observability data:
* log_rcl.py, metric_rcl.py, trace_rcl.py

🔸 FTI (Fault Type Identification)
Classifies fault types using lightweight classification:
* build_data.py: build FTI data based on multi-modal events
* logReg.py: logistic regression training and testing
* logReg_infer_only.py: inference only


## Data

Due to the large size of the datasets, please download them manually from the following link:

👉 Download Datasets: https://drive.google.com/file/d/1eJ5i6PO-h2QzBmZ4-IcZN8QJaodSvWQQ/view?usp=sharing

After downloading, place the extracted folders under the root directory of this project, so the structure looks like this:

```
Metis/
├── Metis-DataSet/
│   ├── Dataset-A/          # Astronomy Shop Dataset
│   └── Dataset-B/          # Train Ticket Dataset
├── AD/
├── RCL/
├── FTI/
...
```


You can modify the dataset directory in the `Metis.py` file by updating the `root_base_dir` variable to point to your desired dataset.

### Example

To use the `Dataset-A`, you would set the following in `Metis.py`:

```python
root_base_dir = Path(r"Metis-DataSet/Dataset-A/")
```

Alternatively, for the `Dataset-B`:

```python
root_base_dir = Path(r"Metis-DataSet/Dataset-B/")
```

## Running the Code

To run the project, simply execute the following command:

```bash
python Metis.py
```

This will execute the code and apply Metis algorithm to the selected dataset.



## Requirements

Make sure to install the necessary Python dependencies before running the project. You can do so by running:

```bash
pip install -r requirements.txt
```
