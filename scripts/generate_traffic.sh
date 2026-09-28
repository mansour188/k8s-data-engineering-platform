#!/bin/bash

BASE_URL="http://localhost:8080"

for i in {1..500}; do

    case $((RANDOM % 5)) in

        0)
            curl -s "$BASE_URL/" > /dev/null
            ;;

        1)
            curl -s "$BASE_URL/api/products" > /dev/null
            ;;

        2)
            curl -s "$BASE_URL/api/orders" > /dev/null
            ;;

        3)
            curl -s "$BASE_URL/api/error" > /dev/null
            ;;

        4)
            curl -s "$BASE_URL/not-found" > /dev/null
            ;;

    esac

done

echo "Generated 500 requests"
