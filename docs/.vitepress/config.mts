import { defineConfig } from 'vitepress'

const providerList =
  'WeCom · Lark · Slack · Telegram · Discord · DingTalk · Matrix · OneBot · QQ · WhatsApp'

const commonHead = [
  ['meta', { name: 'theme-color', content: '#ffffff' }],
  ['meta', { property: 'og:type', content: 'website' }],
  ['meta', { property: 'og:title', content: 'uv-im-connector' }],
  [
    'meta',
    {
      property: 'og:description',
      content: 'Universal IM connector for applications, bots, agents, and automation systems.',
    },
  ],
] as const

const zhSearch = {
  provider: 'local' as const,
  options: {
    locales: {
      root: {
        translations: {
          button: {
            buttonText: '搜索',
            buttonAriaLabel: '搜索文档',
          },
          modal: {
            displayDetails: '显示详情',
            resetButtonTitle: '清除搜索',
            backButtonTitle: '返回',
            noResultsText: '没有找到结果',
            footer: {
              selectText: '选择',
              navigateText: '切换',
              closeText: '关闭',
            },
          },
        },
      },
    },
  },
}

const enSearch = {
  provider: 'local' as const,
}

const zhTheme = {
  search: zhSearch,
  nav: [
    { text: '指南', link: '/guide/getting-started' },
    { text: '架构', link: '/architecture' },
    { text: '参考', link: '/configuration' },
    { text: '贡献', link: '/guide/contributing' },
  ],
  sidebar: [
    {
      text: '指南',
      items: [
        { text: '快速开始', link: '/guide/getting-started' },
        { text: '为什么存在', link: '/guide/why-uv' },
        { text: '核心概念', link: '/guide/concepts' },
        { text: '应用接入', link: '/guide/application-integration' },
        { text: '部署与发布', link: '/guide/deployment' },
        { text: '资源与文件', link: '/guide/resources' },
        { text: '参与贡献', link: '/guide/contributing' },
      ],
    },
    {
      text: '参考',
      items: [
        { text: '架构', link: '/architecture' },
        { text: '配置', link: '/configuration' },
      ],
    },
  ],
  editLink: {
    pattern: 'https://github.com/hengshi/uv-im-connector/edit/main/docs/:path',
    text: '在 GitHub 上编辑此页',
  },
  outline: {
    label: '本页目录',
  },
  docFooter: {
    prev: '上一页',
    next: '下一页',
  },
  lastUpdated: {
    text: '最后更新',
  },
  darkModeSwitchLabel: '外观',
  lightModeSwitchTitle: '切换到浅色模式',
  darkModeSwitchTitle: '切换到深色模式',
  sidebarMenuLabel: '菜单',
  returnToTopLabel: '回到顶部',
  langMenuLabel: '语言',
  socialLinks: [
    { icon: 'github', link: 'https://github.com/hengshi/uv-im-connector' },
  ],
  footer: {
    message: 'Universal IM connector documentation.',
    copyright: 'Copyright © 2026 Hengshi',
  },
}

const enTheme = {
  search: enSearch,
  nav: [
    { text: 'Guide', link: '/en/guide/getting-started' },
    { text: 'Architecture', link: '/en/architecture' },
    { text: 'Reference', link: '/en/configuration' },
    { text: 'Contributing', link: '/en/guide/contributing' },
  ],
  sidebar: [
    {
      text: 'Guide',
      items: [
        { text: 'Getting Started', link: '/en/guide/getting-started' },
        { text: 'Why It Exists', link: '/en/guide/why-uv' },
        { text: 'Concepts', link: '/en/guide/concepts' },
        { text: 'App Integration', link: '/en/guide/application-integration' },
        { text: 'Deployment', link: '/en/guide/deployment' },
        { text: 'Resources', link: '/en/guide/resources' },
        { text: 'Contributing', link: '/en/guide/contributing' },
      ],
    },
    {
      text: 'Reference',
      items: [
        { text: 'Architecture', link: '/en/architecture' },
        { text: 'Configuration', link: '/en/configuration' },
      ],
    },
  ],
  editLink: {
    pattern: 'https://github.com/hengshi/uv-im-connector/edit/main/docs/:path',
    text: 'Edit this page on GitHub',
  },
  outline: {
    label: 'On This Page',
  },
  docFooter: {
    prev: 'Previous Page',
    next: 'Next Page',
  },
  lastUpdated: {
    text: 'Last Updated',
  },
  darkModeSwitchLabel: 'Appearance',
  lightModeSwitchTitle: 'Switch to light mode',
  darkModeSwitchTitle: 'Switch to dark mode',
  sidebarMenuLabel: 'Menu',
  returnToTopLabel: 'Return to top',
  langMenuLabel: 'Language',
  socialLinks: [
    { icon: 'github', link: 'https://github.com/hengshi/uv-im-connector' },
  ],
  footer: {
    message: 'Universal IM connector documentation.',
    copyright: 'Copyright © 2026 Hengshi',
  },
}

export default defineConfig({
  title: 'uv-im-connector',
  description: '面向应用、机器人、Agent 和自动化系统的通用 IM 连接器。',
  lang: 'zh-CN',
  base: '/uv-im-connector/',
  appearance: false,
  lastUpdated: true,
  srcExclude: ['jarvis-box-integration.md', 'DESIGN.md'],
  head: commonHead,
  markdown: {
    attrs: {
      leftDelimiter: '%{',
      rightDelimiter: '}%',
    },
  },
  themeConfig: zhTheme,
  locales: {
    root: {
      label: '简体中文',
      lang: 'zh-CN',
      title: 'uv-im-connector',
      description: `面向应用、机器人、Agent 和自动化系统的通用 IM 连接器，覆盖 ${providerList} 等渠道。`,
      themeConfig: zhTheme,
    },
    en: {
      label: 'English',
      lang: 'en-US',
      title: 'uv-im-connector',
      description: 'Universal IM connector for applications, bots, agents, and automation systems.',
      themeConfig: enTheme,
    },
  },
})
