import streamlit as st
import boto3
import json
import folium
from streamlit_folium import st_folium
import polyline  # pip install polyline

# ---------------- AWS Setup ---------------- #
BUCKET_NAME = "traffic-monitoring-data-lake"
ROUTE_PREFIX = "routes/"
s3 = boto3.client("s3", region_name="eu-north-1")


# ---------------- Helpers ---------------- #
def list_json_files(bucket: str, prefix: str):
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [
            obj["Key"]
            for obj in response.get("Contents", [])
            if obj["Key"].endswith(".json")
        ]
    except Exception as e:
        st.error(f"Error listing files: {e}")
        return []


def load_route_file(bucket: str, key: str):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except Exception as e:
        st.error(f"Error reading {key}: {e}")
        return {}


def parse_coordinates(data: dict):
    if "route" in data:  # case 1: raw list of coordinates
        return data["route"]

    if "geometry" in data:  # case 2: encoded polyline
        try:
            return polyline.decode(data["geometry"])
        except Exception as e:
            st.error(f"Error decoding polyline: {e}")
            return []

    if "routes" in data and data["routes"]:
        summary = data["routes"][0].get("legs", [{}])[0].get("points", [])
        return [(p["latitude"], p["longitude"]) for p in summary if "latitude" in p]

    return []


def create_map(coords: list, route_name: str):
    if not coords:
        return None

    # Make sure coords are floats
    coords = [(float(lat), float(lon)) for lat, lon in coords]

    # Center
    lat_center = sum(lat for lat, lon in coords) / len(coords)
    lon_center = sum(lon for lat, lon in coords) / len(coords)

    m = folium.Map(location=[lat_center, lon_center], zoom_start=6)

    # Add polyline
    folium.PolyLine(coords, color="blue", weight=4, opacity=0.8).add_to(m)

    # Start + End markers
    folium.Marker(coords[0], popup="Start", icon=folium.Icon(color="green")).add_to(m)
    folium.Marker(coords[-1], popup="End", icon=folium.Icon(color="red")).add_to(m)

    return m


# ---------------- Streamlit UI ---------------- #
st.set_page_config(page_title="Traffic Route Visualizer", layout="wide")
st.title("🛣️ Traffic Route Visualizer")

files = list_json_files(BUCKET_NAME, ROUTE_PREFIX)

if not files:
    st.error("No JSON files found in S3.")
else:
    selected_file = st.sidebar.selectbox("Choose a file", files)

    if selected_file:
        data = load_route_file(BUCKET_NAME, selected_file)

        with st.expander("Raw JSON Data"):
            st.json(data)

        coords = parse_coordinates(data)

        st.write(f"✅ Loaded {len(coords)} coordinates")
        if coords:
            st.write("First 5 points:", coords[:5])

        route_name = selected_file.split("/")[-1].replace(".json", "").replace("_", " ").title()

        route_map = create_map(coords, route_name)

        if route_map:
            st_folium(route_map, width=800, height=600)
        else:
            st.warning("No map generated (no coordinates found).")
