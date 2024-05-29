'use client';

import MapStyles from '@/scss/page.module.scss';

import { CaretDownOutlined } from '@ant-design/icons';
import Select from 'antd/lib/select';
import ConfigProvider from 'antd/lib/config-provider';

import Map from '@/components/Map';
import { useCallback, useState } from 'react';

type CityName = keyof typeof cityData;
const provinceData: CityName[] = ['浙江', '江苏'];
const cityData = {
  '浙江': ['杭州', '宁波', '温州'],
  '江苏': ['南京', '苏州', '镇江'],
};

export default function Home() {
  const [cities, setCities] = useState(cityData[provinceData[0] as CityName]);
  const [secondCity, setSecondCity] = useState(cityData[provinceData[0]][0] as CityName);

  const handleProvinceChange = (value: CityName) => {
    setCities(cityData[value]);
    setSecondCity(cityData[value][0] as CityName);
  };

  const onSecondCityChange = (value: CityName) => {
    setSecondCity(value);
  };

  return (
    <main className="relative h-screen">
      <Map />
      <div className={`flex absolute right-0 h-screen p-4 w-1/3 ${MapStyles['panel']}`}>
        <div className={`${MapStyles['panel-expander']}`}></div>
        <div className={`flex w-full p-5 ${MapStyles['panel-container']}`}>
          <ConfigProvider
            theme={{
              token: {
                colorBgContainer: MapStyles.panelSearchBgColor,
                colorBgElevated: MapStyles.panelSearchBgColor,
                colorText: MapStyles.normalFontColor,
                colorTextPlaceholder: MapStyles.placeholderFontColor,
                colorPrimary: MapStyles.panelSearchBtnBgColor,
                colorPrimaryActive: MapStyles.panelSearchBtnHoverColor,
                colorPrimaryHover: MapStyles.panelSearchBtnHoverColor,
                colorTextQuaternary: MapStyles.normalFontColor,
                controlHeight: 35,
                borderRadiusLG: 6,
                lineWidth: 0,
              }
            }}>
            <Select
              style={{marginRight: '.75rem', width: '25%', minWidth: '100px'}}
              defaultValue={provinceData[0]}
              onChange={handleProvinceChange}
              options={provinceData.map((province) => ({ label: province, value: province }))}
              size='large'
              placeholder='省/市/区'
              suffixIcon={<CaretDownOutlined />}
            />
            <Select
              className='flex-1'
              value={secondCity}
              onChange={onSecondCityChange}
              options={cities.map((city) => ({ label: city, value: city }))}
              size='large'
              placeholder='城市'
              suffixIcon={<CaretDownOutlined />}
            />
          </ConfigProvider>
        </div>
      </div>
    </main>
  );
}
