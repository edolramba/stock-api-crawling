@ECHO ON
title startDataCrawler Start

cd C:\Dev\stock-api-crawling
call workon stock-api-crawling

python dataCrawler.py

exit