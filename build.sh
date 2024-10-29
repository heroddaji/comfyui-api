#! /usr/bin/env bash

npm install
npm run build-binary
cp ./bin/comfyui-api ../genart-flux_schnell_q4_k_s-saladcloud/comfyui_api_wrapper/bin/