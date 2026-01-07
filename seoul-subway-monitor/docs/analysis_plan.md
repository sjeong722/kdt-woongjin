# Seoul Subway Data Analysis Projects

This document outlines specific data analysis projects using the collected real-time subway position data. The primary goal is to monitor for smooth subway operations.

## 1. Interval Regularity Monitoring (배차 간격 모니터링)

### Objective
Ensure that trains are spacing themselves evenly according to the schedule, preventing "bunching" (trains arriving too close together) or "gapping" (long wait times).

### Methodology
1.  **Data Preparation**:
    *   Group data by `line_id` and `direction_type` (Up/Down).
    *   Sort by `station_id` sequence to determine the order of trains on the track.
2.  **Metric Calculation**:
    *   Calculate the distance (in terms of stations) or time difference between adjacent trains.
    *   `Time Gap = ArrivalTime(Train B @ Station X) - ArrivalTime(Train A @ Station X)`
3.  **Alert Logic**:
    *   Define a threshold for "Regular Interval" (e.g., peak hours 2-3 min, off-peak 5-8 min).
    *   Flag instances where `Time Gap` deviates by > 50% of the scheduled interval.

### Outcome
*   Dashboard showing real-time "Health Score" of dispatch intervals per line.
*   Alerts for specific line sections where bunching is occurring.

## 2. Delay Hotspot Detection (지연 발생 구간 탐지)

### Objective
Identify specific stations or sections where trains consistently spend more time than expected, indicating signaling issues, passenger congestion, or mechanical problems.

### Methodology
1.  **Data Preparation**:
    *   Track the lifecycle of a single train (`train_number`) as it moves.
    *   Record the timestamp when `train_status` changes from 1 (Arrive) to 2 (Depart).
2.  **Metric Calculation**:
    *   `Dwell Time = DepartTime - ArriveTime`
    *   `Section Travel Time = ArriveTime(Station B) - DepartTime(Station A)`
3.  **Analysis**:
    *   Calculate Moving Average of Dwell Time per station.
    *   Compare Real-time Dwell Time vs. Historical Average.

### Outcome
*   Heatmap of Seoul Subway map highlighting "Slow Zones".
*   Weekly report identifying "Worst Congestion Stations".

## 3. Terminal Station Turnaround Efficiency (종착역 회차 효율성 분석)

### Objective
Monitor how quickly trains turn around at terminal stations to re-enter service, which is critical for maintaining schedule capability during rush hours.

### Methodology
1.  **Data Filter**:
    *   Filter for trains where `station_name` == `dest_station_name` (Approaching terminal).
2.  **Metric Calculation**:
    *   Measure time from "Arrival at Terminal" to "Departure as new Train Number" (if linkable) or simply "Time Occupying Platform".
3.  **Analysis**:
    *   Analyze turnaround times by hour of day.

### Outcome
*   Operational efficiency metric for terminal stations (e.g., Wangsimni, Sadang).

## 4. Real-time Phantom Jam Detection (유령 체증 탐지)

### Objective
Detect "Phantom Jams" where a train stops or slows down between stations (status 0: Enter or 3: Pre-depart) for an extended period without a station stop.

### Methodology
1.  **Data Monitoring**:
    *   Watch rows where `train_status` remains 0 (Moving/Entering) for generic duration > Threshold (e.g., > 3 mins).
2.  **Correlation**:
    *   Check if the preceding train is still at the next station (Cause of delay).

### Outcome
*   Real-time warning system for "Stuck Trains".
