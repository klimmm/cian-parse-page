#!/bin/bash

# Script to kill stuck bird process when CPU usage is too high
# Run this in background during scraping: ./kill_stuck_bird.sh &

while true; do
    # Get system CPU usage from top
    SYSTEM_STATS=$(top -l 1 | grep "CPU usage" | head -1)
    
    # Get bird process CPU usage (only the first/highest one)
    BIRD_CPU=$(ps aux | grep '[b]ird' | head -1 | awk '{print $3}')
    
    # Get top 3 CPU processes (macOS compatible)
    TOP_PROCESSES=$(ps aux | sort -k3 -nr | head -3 | awk '{printf "%s(%.1f%%) ", $11, $3}')
    
    # Show current system status
    echo "$(date '+%H:%M:%S'): $SYSTEM_STATS"
    echo "               Top processes: $TOP_PROCESSES"
    
    if [ ! -z "$BIRD_CPU" ]; then
        # Convert to integer for comparison
        BIRD_CPU_INT=$(echo "$BIRD_CPU" | awk '{print int($1)}')
        
        echo "               Bird CPU: ${BIRD_CPU}%"
        
        # Check if it's a valid number and greater than 50
        if [[ "$BIRD_CPU_INT" =~ ^[0-9]+$ ]] && [ "$BIRD_CPU_INT" -gt 50 ]; then
            echo "               🔪 KILLING bird process (${BIRD_CPU}% CPU)"
            killall bird
            sleep 2
        fi
    else
        echo "               Bird: not running"
    fi
    
    echo "               ────────────────────────────────────"
    sleep 10
done