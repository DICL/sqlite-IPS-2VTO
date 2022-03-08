stop mpdecision
for i in 0 1 2 3 4 5 6 7
do
	echo "performance" > "/sys/devices/system/cpu/cpu$i/cpufreq/scaling_governor"
	V=0
	while [[ $V -lt 1950000 ]]
	do
		echo "1950000" > "/sys/devices/system/cpu/cpu$i/cpufreq/scaling_min_freq"
		V=$((`cat /sys/devices/system/cpu/cpu$i/cpufreq/cpuinfo_cur_freq`))
	done
done