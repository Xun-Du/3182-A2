# 3182-A2

## Execution Steps & Order of Operations

> **CRITICAL NOTE (Execution Sequence):**
> Because the Spark streaming source is configured with `"startingOffsets": "latest"`, you **MUST first start the streaming query (Step 1)** so that it is warm and listening, **and then trigger the Kafka producers (Step 2)**. Reversing this order will cause Spark to miss the initial batch of records emitted by the producers.

### 1️. Launch the Spark Streaming Core Engine

- Open the `34248773_34220097_data_design_streaming.ipynb` notebook.
- **Run all cells sequentially**.
- This action compiles and activates the Stream-Stream Joins, Data Quality Filters, and unified MongoDB/Console output sinks.
- The Driver thread will block and enter a real-time listening state. You will see the following confirmation log:
  `[INFO] Starting synchronized dual-stream architecture...`

---

### 2️. Trigger the Kafka Producers

Once the streaming engine is up and actively listening, you can activate the producers to push real-time streams:

- Open and run all cells in `34248773_34220097_producer_a.ipynb` (Camera A data stream).
- Open and run all cells in `34248773_34220097_producer_b.ipynb` (Camera B data stream).
- Open and run all cells in `34248773_34220097_producer_c.ipynb` (Camera C data stream).

> _At this point, you can monitor the real-time payload inside each producer's notebook terminal._

---

### 3️. Real-Time Visualisation Interface

After the Spark streaming engine is running and the Kafka producers have started sending camera events, open the visualisation notebook:

- `34248773_34220097_visualisation.ipynb`

Run all cells sequentially. The notebook connects to the MongoDB `violations` collection and continuously polls for newly inserted violation records.

The visualisation dashboard produces two live line charts:

1. **Violation Count over Event Time**
   - Shows the number of detected violations over time.
   - Uses separate lines for instantaneous-speed violations and average-speed violations.
   - Helps answer when violation activity peaks and whether one violation type is more frequent than the other.

2. **Speed Pattern over Event Time**
   - Shows the mean violation speed over time.
   - Includes a moving average line to make short-term speed trends easier to interpret.
   - Helps identify whether violation speeds are increasing, decreasing, or staying stable during the monitoring period.

The dashboard also marks important analytical points, including:

- maximum and minimum values,
- sudden spike regions,
- dynamically calculated percentile thresholds,
- speed-limit reference lines where applicable.

These annotations are included to support operational monitoring. For example, peaks and spike regions can indicate unusual traffic behaviour, while percentile thresholds help distinguish normal variation from high-risk periods.

If no violation records have been written to MongoDB yet, the notebook will wait until records become available. This usually means the Spark streaming notebook or Kafka producers have not started yet, or the producers have not emitted enough records.

To stop the visualisation dashboard, click **Interrupt Kernel** in Jupyter Notebook.

### 4️. Graceful Shutdown of the Data Pipeline

When testing is complete, do not violently close the terminal or crash the kernels. The streaming driver is protected by a robust lifecycle interceptor:

- In the `34248773_34220097_data_design_streaming.ipynb` notebook, click **"Interrupt the Kernel"**
- The system's `try-except-finally` block will safely catch the interrupt signal, flush remaining in-flight micro-batches, release active cluster resources, and output the following clean shutdown log:
  ```text
  Interrupted by CTRL-C. Stopped query
  [SUCCESS] MongoDB Stream writer stopped.
  [SUCCESS] Console Logger Stream stopped.
  [INFO] Streaming application terminated cleanly.
  ```

---

## GenAI Usage Statement

In compliance with the academic integrity guidelines, this section outlines the utilization of Generative AI tools during the development of this project.

### Purpose of Use

Generative AI was used strictly to help me organize my thoughts and offer suggestions. The core objective was to explore complex streaming mechanics, understand state-store restrictions, and design a scalable, production-grade database schema. **No functional code or logic was blindly copied.** Every pipeline phase was manually integrated, refactored, and thoroughly tested within the local containerized cluster to guarantee operational and logical validity.

### Prompt Summary

- How to read JSON data from Kafka in PySpark and convert the string timestamp into an actual Spark timestamp format?
- Why does PySpark throw an AnalysisException saying stream-stream join is not supported in Update mode? How to fix it if I need to do a Left Join?
- How to use foreachBatch in Spark to update a single daily document for a car in MongoDB, and use $addToSet to avoid duplicate data when a batch retries?
- How to set a 10-minute watermark and join interval in PySpark so that Camera A events wait for Camera B events based on event time?
- In PySpark streaming, how can I calculate vehicle speed by dividing distance by time difference, but safely ignore rows where the time difference is 0 or negative to avoid crash?
- How do I create a TTL index in MongoDB using PyMongo to automatically delete documents after 5 years based on a date field?
- How to use a Left Outer Join in Spark streaming to find vehicles that entered Camera A but never showed up at Camera B, and print them to the console?
- How to wrap `query.awaitTermination()` in a python try-except block so that when I press the stop/interrupt button in Jupyter, it stops the stream safely without showing ugly error tracks?
- Help me to organize the docstrings of these function.
- How can I design the visualisation notebook to show violation counts and speed patterns over event time?
- How can I annotate maxima, minima, sudden spikes, shaded regions, and percentile thresholds in Matplotlib?
- How can I adjust Matplotlib/Jupyter Notebook rendering so the live dashboard is readable during execution?
