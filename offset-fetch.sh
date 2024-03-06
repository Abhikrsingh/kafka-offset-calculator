#!/bin/bash


filterBasedOn=$1

fetch_kafka_topics() {
    # Run kafka-topics.sh command and store the output in an array
    fetch_kafka_topics=($(/opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092))

    # Print fetched Kafka topics
    #echo "Fetched Kafka topics:"
    #printf '%s\n' "${fetch_kafka_topics[@]}"
    echo -n "$(date '+%Y-%m-%d %H:%M:%S') !~! Fetched Kafka topics: ("
    printf '"%s" ' "${fetch_kafka_topics[@]}"
    echo ")"
}



filter_kafka_topics() {


    ignore_kafka_topics=("__consumer_offsets" "first-topic")



    # Filter fetched Kafka topics
    filtered_kafka_topics=()
    for topic in "${fetch_kafka_topics[@]}"; do
        if [[ ! " ${ignore_kafka_topics[@]} " =~ " ${topic} " ]]; then
            filtered_kafka_topics+=("$topic")

	
        fi
    done

    # Print filtered Kafka topics
    #echo "Filtered Kafka topics:"
    #printf '%s\n' "${filtered_kafka_topics[@]}"

    echo -n "$(date '+%Y-%m-%d %H:%M:%S') !~! Filtered Kafka topics: ("
    printf '"%s" ' "${filtered_kafka_topics[@]}"
    echo ")"
}




selected_kafka_topics() {
    # Define Kafka topics to fetch data for
    filtered_kafka_topics=("topic-1" "topic-2" "topic-3" "topic-4")

    # Print selected Kafka topics as an array
    echo -n "$(date '+%Y-%m-%d %H:%M:%S') !~! Selected Kafka topics: ("
    printf '"%s" ' "${filtered_kafka_topics[@]}"
    echo ")"
}



create_json_output() {
    # Initialize JSON object
    json="{"

    # Loop through each Kafka topic
    for topic in "${filtered_kafka_topics[@]}"; do
        # Run kafka-run-class.sh command and store the output in a variable
        output=$(/opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic "$topic" --time -1)

        # Initialize an array to hold partition data
        partitions=()

        # Loop through each line of the output
        while IFS= read -r line; do
            # Split the line by ":" to get topic name, partition, and last offset
            IFS=':' read -r -a array <<< "$line"
            partition="${array[1]}"
            lastoffset="${array[2]}"

            # Add partition data to the partitions array
            partitions+=("{\"partition\": $partition, \"lastoffset\": $lastoffset}")
        done <<< "$output"

        # Convert the partitions array to JSON format
        partitions_json=$(IFS=,; echo "${partitions[*]}")

        # Create the topic JSON object
        topic_json="\"$topic\": [${partitions_json}]"

        # Add topic JSON to the main JSON object
        if [ "$json" == "{" ]; then
            json="$json$topic_json"
        else
            json="$json, $topic_json"
        fi
    done

    # Close the JSON object
    json="$json}"

    # Print the JSON output
    echo "$(date '+%Y-%m-%d %H:%M:%S') !~! JSON output: $json"

    echo "$json" > /opt/offset/offset-$(date '+%Y-%m-%d-%H').json
}




sumOfPartition() {

    # Get the current hour and one hour back
    current_hour=$(date +'%Y-%m-%d-%H')
    one_hour_back=$(date -d '1 hour ago' +'%Y-%m-%d-%H')

    # Generate file names
    current_hour_file="/opt/offset/offset-$current_hour.json"
    one_hour_back_file="/opt/offset/offset-$one_hour_back.json"

    # Array of file names containing JSON data
    partitionInfo=("$one_hour_back_file" "$current_hour_file")

    # Initialize an empty JSON object
    output="{"

    # Loop through each file
    for file in "${partitionInfo[@]}"; do
        # Extract the file name without the directory path and extension
        filename=$(basename "$file" .json)
        # Trim the "offset-" prefix from the filename to get the desired key
        key=$(echo "$filename" | sed 's/offset-//')

        # Read keys from the JSON file
        keys=$(jq -r 'keys | .[]' "$file")

        # Initialize an array for the current file
        output="$output \"$key\": ["

        # Loop through each key
        for key in $keys; do
            # Extract array for the current key and sum up lastoffset values
            sum=$(jq -r --arg key "$key" '.[$key][] | .lastoffset' "$file" | awk '{ sum += $1 } END { print sum }')

            # Append the key and sum to the array
            output="$output { \"$key\": $sum },"
        done

        # Remove the trailing comma and close the array
        output="${output%,}]"

        # Append a comma to separate file entries
        output="$output,"
    done

    # Remove the trailing comma and close the JSON object
    output="${output%,}}"
    echo "$(date '+%Y-%m-%d %H:%M:%S') !~! $output"
    sumJson=$output
    echo "$sumJson" > /opt/offset/offset-sum-$(date '+%Y-%m-%d-%H').json
}




if [ "$filterBasedOn" == "all" ]
then

         fetch_kafka_topics
         filter_kafka_topics
         create_json_output
	 sumOfPartition

elif [ "$filterBasedOn" == "selected" ]
then
         selected_kafka_topics
         create_json_output
	 sleep 15
	 sumOfPartition
else
         echo "wrong input"
	
fi
