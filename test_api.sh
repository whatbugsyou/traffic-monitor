#!/bin/bash
# Traffic Monitor - API Test Script

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Test configuration
SERVER_URL="http://localhost:8080"
TIMEOUT=5
TEST_NAMESPACE="default"

# Test counters
TESTS_PASSED=0
TESTS_FAILED=0
TESTS_TOTAL=0

# Print test header
print_header() {
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Traffic Monitor API Tests${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "${CYAN}Server: $SERVER_URL${NC}"
    echo -e "${CYAN}Time:   $(date)${NC}"
    echo ""
}

# Print test result
print_result() {
    local test_name="$1"
    local result="$2"
    local details="$3"

    TESTS_TOTAL=$((TESTS_TOTAL + 1))

    if [ "$result" == "PASS" ]; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        echo -e "${GREEN}✓ PASS${NC}: $test_name"
        if [ -n "$details" ]; then
            echo -e "  ${CYAN}$details${NC}"
        fi
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        echo -e "${RED}✗ FAIL${NC}: $test_name"
        if [ -n "$details" ]; then
            echo -e "  ${YELLOW}$details${NC}"
        fi
    fi
}

# Test server health
test_server_health() {
    echo -e "${CYAN}Testing server health...${NC}"

    # Check if server is running
    if curl -s --max-time $TIMEOUT "$SERVER_URL/api/namespaces" > /dev/null 2>&1; then
        print_result "Server is running" "PASS"
    else
        print_result "Server is running" "FAIL" "Cannot connect to $SERVER_URL"
        echo -e "${YELLOW}Hint: Start server with ./run.sh start${NC}"
        exit 1
    fi
}

# Test GET /api/namespaces
test_get_namespaces() {
    echo -e "${CYAN}Testing GET /api/namespaces...${NC}"

    local response
    response=$(curl -s --max-time $TIMEOUT "$SERVER_URL/api/namespaces")

    # Check if response is valid JSON
    if echo "$response" | jq -e . > /dev/null 2>&1; then
        print_result "Valid JSON response" "PASS"

        # Check if namespaces array exists
        if echo "$response" | jq -e '.namespaces' > /dev/null 2>&1; then
            print_result "Namespaces array exists" "PASS"

            # Check if default namespace exists
            if echo "$response" | jq -e '.namespaces | contains(["default"])' > /dev/null 2>&1; then
                print_result "Default namespace exists" "PASS"
            else
                print_result "Default namespace exists" "FAIL" "Default namespace not found"
            fi

            # Count namespaces
            local count
            count=$(echo "$response" | jq '.namespaces | length')
            print_result "Namespace count: $count" "PASS"
        else
            print_result "Namespaces array exists" "FAIL" "Array not found in response"
        fi
    else
        print_result "Valid JSON response" "FAIL" "Invalid JSON: $response"
    fi
}

# Test GET /api/current
test_get_current() {
    echo -e "${CYAN}Testing GET /api/current...${NC}"

    local response
    response=$(curl -s --max-time $TIMEOUT "$SERVER_URL/api/current?namespace=$TEST_NAMESPACE")

    # Check if response is valid JSON
    if echo "$response" | jq -e . > /dev/null 2>&1; then
        print_result "Valid JSON response" "PASS"

        # Check required fields
        local fields=("namespace" "timestamp" "timestamp_ms" "interfaces" "ppp0")
        for field in "${fields[@]}"; do
            if echo "$response" | jq -e ".$field" > /dev/null 2>&1; then
                print_result "Field '$field' exists" "PASS"
            else
                print_result "Field '$field' exists" "FAIL" "Field not found"
            fi
        done

        # Check namespace value
        local ns
        ns=$(echo "$response" | jq -r '.namespace')
        if [ "$ns" == "$TEST_NAMESPACE" ]; then
            print_result "Namespace is correct" "PASS"
        else
            print_result "Namespace is correct" "FAIL" "Expected: $TEST_NAMESPACE, Got: $ns"
        fi

        # Check timestamp_ms is numeric
        if echo "$response" | jq -e '.timestamp_ms | type == "number"' > /dev/null 2>&1; then
            print_result "timestamp_ms is numeric" "PASS"
        else
            print_result "timestamp_ms is numeric" "FAIL" "Not a number"
        fi

        # Check interfaces is array
        if echo "$response" | jq -e '.interfaces | type == "array"' > /dev/null 2>&1; then
            print_result "interfaces is array" "PASS"

            # Check interface count
            local iface_count
            iface_count=$(echo "$response" | jq '.interfaces | length')
            print_result "Interface count: $iface_count" "PASS"
        else
            print_result "interfaces is array" "FAIL" "Not an array"
        fi

    else
        print_result "Valid JSON response" "FAIL" "Invalid JSON: $response"
    fi
}

# Test GET /api/history
test_get_history() {
    echo -e "${CYAN}Testing GET /api/history...${NC}"

    # Test different durations
    local durations=(5 10 30 60 120 180)

    for duration in "${durations[@]}"; do
        local response
        response=$(curl -s --max-time $TIMEOUT "$SERVER_URL/api/history?namespace=$TEST_NAMESPACE&duration=$duration")

        if echo "$response" | jq -e . > /dev/null 2>&1; then
            local count
            count=$(echo "$response" | jq '.data | length')

            if [ "$count" -ge 0 ]; then
                print_result "History (duration=${duration}min)" "PASS" "Data points: $count"
            else
                print_result "History (duration=${duration}min)" "FAIL" "Invalid data count"
            fi
        else
            print_result "History (duration=${duration}min)" "FAIL" "Invalid JSON"
        fi
    done
}

# Test GET /api/stream (SSE)
test_sse_stream() {
    echo -e "${CYAN}Testing GET /api/stream (SSE)...${NC}"

    # Test SSE stream for 3 seconds
    local timeout_secs=3
    local received_data=false

    if timeout $timeout_secs curl -N -s "$SERVER_URL/api/stream?namespace=$TEST_NAMESPACE" 2>&1 | grep -q "data:"; then
        received_data=true
    fi

    if [ "$received_data" = true ]; then
        print_result "SSE stream receives data" "PASS" "Received SSE event"
    else
        print_result "SSE stream receives data" "FAIL" "No SSE event received in ${timeout_secs}s"
    fi
}

# Test CORS headers
test_cors() {
    echo -e "${CYAN}Testing CORS headers...${NC}"

    local headers
    headers=$(curl -s -I --max-time $TIMEOUT "$SERVER_URL/api/namespaces" 2>&1)

    if echo "$headers" | grep -qi "Access-Control-Allow-Origin"; then
        print_result "CORS headers present" "PASS"
    else
        print_result "CORS headers present" "FAIL" "No CORS headers found"
    fi
}

# Test error handling
test_error_handling() {
    echo -e "${CYAN}Testing error handling...${NC}"

    # Test non-existent namespace
    local response
    response=$(curl -s --max-time $TIMEOUT -o /dev/null -w "%{http_code}" "$SERVER_URL/api/current?namespace=nonexistent")

    if [ "$response" == "404" ] || [ "$response" == "200" ]; then
        print_result "Non-existent namespace handling" "PASS" "HTTP $response"
    else
        print_result "Non-existent namespace handling" "FAIL" "Unexpected HTTP code: $response"
    fi

    # Test invalid duration
    response=$(curl -s --max-time $TIMEOUT -o /dev/null -w "%{http_code}" "$SERVER_URL/api/history?duration=invalid")

    if [ "$response" == "200" ] || [ "$response" == "400" ]; then
        print_result "Invalid duration handling" "PASS" "HTTP $response"
    else
        print_result "Invalid duration handling" "FAIL" "Unexpected HTTP code: $response"
    fi
}

# Test performance
test_performance() {
    echo -e "${CYAN}Testing performance...${NC}"

    # Test response time for /api/namespaces
    local time_ms
    time_ms=$(curl -s -o /dev/null -w "%{time_total}" --max-time $TIMEOUT "$SERVER_URL/api/namespaces")
    time_ms=$(echo "$time_ms * 1000" | bc)
    time_ms=${time_ms%.*}  # Remove decimal

    if [ "$time_ms" -lt 100 ]; then
        print_result "Response time < 100ms" "PASS" "${time_ms}ms"
    else
        print_result "Response time < 100ms" "FAIL" "${time_ms}ms (too slow)"
    fi

    # Test concurrent requests
    local concurrent=10
    local success=true

    for i in $(seq 1 $concurrent); do
        curl -s --max-time $TIMEOUT "$SERVER_URL/api/namespaces" > /dev/null &
    done

    wait

    print_result "Concurrent requests ($concurrent)" "PASS" "All requests completed"
}

# Test data consistency
test_data_consistency() {
    echo -e "${CYAN}Testing data consistency...${NC}"

    # Get current timestamp
    local response1
    response1=$(curl -s --max-time $TIMEOUT "$SERVER_URL/api/current?namespace=$TEST_NAMESPACE")

    sleep 2

    local response2
    response2=$(curl -s --max-time $TIMEOUT "$SERVER_URL/api/current?namespace=$TEST_NAMESPACE")

    local ts1 ts2
    ts1=$(echo "$response1" | jq -r '.timestamp_ms')
    ts2=$(echo "$response2" | jq -r '.timestamp_ms')

    if [ "$ts2" -gt "$ts1" ]; then
        print_result "Timestamp increases over time" "PASS" "ts1=$ts1, ts2=$ts2"
    else
        print_result "Timestamp increases over time" "FAIL" "Timestamps: ts1=$ts1, ts2=$ts2"
    fi
}

# Print test summary
print_summary() {
    echo ""
    echo -e "${BLUE}========================================${NC}"
    echo -e "${BLUE}Test Summary${NC}"
    echo -e "${BLUE}========================================${NC}"
    echo -e "Total:  $TESTS_TOTAL"
    echo -e "${GREEN}Passed: $TESTS_PASSED${NC}"
    echo -e "${RED}Failed: $TESTS_FAILED${NC}"

    if [ $TESTS_FAILED -eq 0 ]; then
        echo -e "${GREEN}All tests passed! ✓${NC}"
        exit 0
    else
        echo -e "${RED}Some tests failed! ✗${NC}"
        exit 1
    fi
}

# Main test execution
main() {
    print_header

    # Check dependencies
    if ! command -v jq &> /dev/null; then
        echo -e "${RED}Error: jq is not installed${NC}"
        echo -e "${YELLOW}Install with: apt-get install jq${NC}"
        exit 1
    fi

    if ! command -v bc &> /dev/null; then
        echo -e "${RED}Error: bc is not installed${NC}"
        echo -e "${YELLOW}Install with: apt-get install bc${NC}"
        exit 1
    fi

    # Run tests
    test_server_health
    test_get_namespaces
    test_get_current
    test_get_history
    test_sse_stream
    test_cors
    test_error_handling
    test_performance
    test_data_consistency

    # Print summary
    print_summary
}

# Run main function
main "$@"
