import folium 
import pandas as pd



# define the world map
world_map = folium.Map()

# display world map
world_map 


# San Francisco latitude and longitude values
latitude = 37.77
longitude = -122.42

# Create map and display it
san_map = folium.Map(location=[latitude, longitude], zoom_start=12)

# Display the map of San Francisco
san_map

