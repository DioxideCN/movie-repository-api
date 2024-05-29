"use client";

import React, { useEffect } from 'react';

const Map = React.memo(() => {
  useEffect(() => {
    // @ts-ignore
    window._AMapSecurityConfig = {
      securityJsCode: "672573f1dd649f7342883bf2fb13c033",
    };
    import('@amap/amap-jsapi-loader').then(AMapLoader => {
      AMapLoader.load({
          key: "9701a51c8b5cad9d0c6f97c568a553a9",
          version: "2.0",
        })
        .then((AMap) => {
          var map = new AMap.Map("map", {
            zoom: 15,
            viewMode: "2D",
            mapStyle: "amap://styles/5688580c51728e1beabfaf9714d3cc3b",
          });
          AMap.plugin([
            "AMap.Geolocation",
          ], function () {
            var geo = new AMap.Geolocation();
          });
          var contextMenu = new AMap.ContextMenu();
          contextMenu.addItem("放大", function () {
            map.zoomIn();
          }, 0);
          contextMenu.addItem("缩小", function () {
            map.zoomOut();
          }, 1);
          map.on('rightclick', function (e: any) {
            contextMenu.open(map, e.lnglat);
          });
        })
        .catch((e) => {
          console.log(e);
        });
    })
  })

  return (
    <div id="map" className="w-full h-full absolute inset-x-0 inset-y-0"></div>
  );
});

export default Map;
