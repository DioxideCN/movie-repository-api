"use client";

import LogoSVG from '../../../../public/logo.svg';
import AuthStyles from '@/scss/page.module.scss';

import Divider from 'antd/lib/divider';
import Button from 'antd/lib/button';
import ConfigProvider from 'antd/lib/config-provider';
import Input from 'antd/lib/input/Input';
import Password from 'antd/lib/input/Password';

export default function AuthPage() {
  return (
    <main className="flex flex-col h-screen p-3">
      <section className="flex h-screen items-center justify-center p-8">
        <div className="flex flex-col items-center justify-center">
          <div className="flex">
            <LogoSVG viewBox="0 0 560 560" width={AuthStyles.authTitleHeight} height={AuthStyles.authTitleHeight} />
            <text className={`ml-4 font-bold ${AuthStyles['auth-head--title']}`}>Momentum</text>
          </div>
          <div className="flex flex-col w-full items-center justify-center">
            <ConfigProvider
                theme={{
                  token: {
                    colorText: AuthStyles.subFontColor,
                    colorSplit: 'transparent',
                  },
                }}>
              <Divider plain>该版本仅用于冯鑫小组课设演示</Divider>
            </ConfigProvider>
            <ConfigProvider
                theme={{
                  components: {
                    Input: {
                      activeBorderColor: AuthStyles.activeColor,
                      hoverBorderColor: AuthStyles.activeColor,
                      activeShadow: AuthStyles.activeShadow,
                    },
                  },
                  token: {
                    colorBgContainer: 'transparent',
                    colorIcon: AuthStyles.subFontColor,
                    colorTextPlaceholder: AuthStyles.subFontColor,
                    colorBorder: AuthStyles.mainFontColor,
                    colorText: AuthStyles.mainFontColor,
                    lineWidth: 2,
                  },
                }}>
              <Input className="mb-6" size="large" placeholder="账号 / 邮箱" allowClear />
              <Password className="mb-6" size="large" placeholder="密码" />
            </ConfigProvider>
            <ConfigProvider
                theme={{
                  components: {
                    Button: {
                      defaultBorderColor: AuthStyles.midFontColor,
                      defaultHoverBorderColor: AuthStyles.midFontColor,
                      defaultActiveBorderColor: AuthStyles.midFontColor,
                      defaultHoverBg: AuthStyles.lightFontColor,
                      defaultHoverColor: AuthStyles.white,
                    },
                  },
                  token: {
                    colorBorder: AuthStyles.midFontColor,
                    colorBgContainer: AuthStyles.midFontColor,
                    colorText: AuthStyles.white,
                    lineWidth: 0,
                  },
                }}>
              <Button className="w-full" size="large">登录</Button>
            </ConfigProvider>
            <ConfigProvider
                theme={{
                  token: {
                    colorText: AuthStyles.subFontColor,
                    colorSplit: AuthStyles.subFontColor,
                  },
                }}>
              <Divider plain>第三方登录</Divider>
            </ConfigProvider>
          </div>
        </div>
      </section>
      <footer className={`w-full p-4 text-center text-xs ${AuthStyles['auth-footer']}`}>
        <a href='https://beian.miit.gov.cn/' target='_blank'>苏ICP备2021003727号</a>, Designed by <a href='https://dioxide-cn.ink/' target='_blank'>DioxideCN</a>, Powered by <a href='https://www.nextjs.cn/' target='_blank'>Next.js</a>
      </footer>
    </main>
  );
}
