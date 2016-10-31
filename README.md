# monitoring-configurator
A prototype for my thesis "Monitoring in NFV: Efficient delivery of monitoring data"

#Basic principle
To facilitate the transmission of large volumes of (near) real-time monitoring data in dynamic cloud environments 
I explore the possibility of using flat virtual local area networks that can span over several cloud installations.
Thanks to the network virtualization all underlying complexities can be hidden away from the virtual machines participating in
performance monitoring (both metric producers and processing nodes). 
The fact that the used network has the simplest topology and uniform address space 
allows the prototype to conduct automated node discovery using simple UDP broadcast. 

