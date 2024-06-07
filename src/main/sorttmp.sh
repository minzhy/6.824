cat mr-tmp/tmp-mr-out-"$1" | sort > mr-tmp/a
cat my-tmp/tmp-mr-out-"$1" | sort > my-tmp/a
diff mr-tmp/a my-tmp/a