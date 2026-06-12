export default {
  title: "BEC Manual",
  description: "User manual for the BEC automated trading app.",
  base: "/BEC/",
  ignoreDeadLinks: false,
  themeConfig: {
    nav: [
      { text: "Manual", link: "/" },
      { text: "GitHub", link: "https://github.com/jptsantossilva/BEC" },
    ],
    sidebar: [
      { text: "Overview", link: "/" },
      { text: "Getting Started", link: "/getting-started" },
      { text: "Dashboard", link: "/dashboard" },
      { text: "Market Analysis", link: "/market-analysis" },
      { text: "Backtesting", link: "/backtesting" },
      { text: "Monte Carlo Analysis", link: "/monte-carlo" },
      { text: "Updates", link: "/updates" },
      { text: "Troubleshooting", link: "/troubleshooting" },
    ],
    socialLinks: [
      { icon: "github", link: "https://github.com/jptsantossilva/BEC" },
    ],
    footer: {
      message: "Educational software only. Use at your own risk.",
      copyright: "MIT Licensed. Copyright © 2026",
    },
    search: {
      provider: "local",
    },
  },
};
