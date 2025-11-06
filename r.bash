#!/bin/bash

rsync -az -e ssh "/mnt/ai/cnn/ai_report" root@82.114.2.2:/usr/local/projects/ai_report
