!/bin/bash
for i in ../archive2/*.txt
do
  tar -c -T "${i}" | gzip -1 > "${i%.txt}.tar.gz"
done

