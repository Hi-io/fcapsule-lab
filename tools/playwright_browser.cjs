function launchChromium(chromium, environment = process.env) {
  const options = { headless: true };
  if (environment.CHROME_EXECUTABLE) options.executablePath = environment.CHROME_EXECUTABLE;
  return chromium.launch(options);
}

module.exports = { launchChromium };
