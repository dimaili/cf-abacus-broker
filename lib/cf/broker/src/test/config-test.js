'use strict';

let config = require('../config.js');

describe('Config', () => {

  context('FIXME', () => {
    const abacusRpPrefix = 'abacus-rp-';

    it('should set a prefix to a parameter specified', () => {
      const id = '123';
      expect(config.resourceProviderPrefix(id))
        .to.equal(`${abacusRpPrefix}${id}`);
    });

    it('should return the prefix when there is no parameter specified', () => {
      expect(config.resourceProviderPrefix()).to.equal(abacusRpPrefix);
    });
  });

  context('FIXME', () => {
    beforeEach(() => {
      delete process.env.RESOURCE_USAGE_PATH;
      delete require.cache[require.resolve('../config.js')];
    });

    it('should read collector usage path from the environment', () => {
      const path = '/some/path';
      process.env.RESOURCE_USAGE_PATH = path;
      config = require('../config.js');
      expect(config.resourceUsagePath).to.equal(path);
    });

    it('should return default when no varaible is specified',() => {
      config = require('../config.js');
      expect(config.resourceUsagePath).to.equal('/v1/metering/collected/usage');
    });
  });
});
