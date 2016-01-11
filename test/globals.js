'use strict';

/* eslint vars-on-top: 0 */

global.sinon = require('sinon');
global.chai = require('chai');

global.assert = global.chai.assert;
global.expect = global.chai.expect;
global.should = global.chai.should();

// https://github.com/domenic/chai-as-promised
var chaiAsPromised = require('chai-as-promised');
global.chai.use(chaiAsPromised);

// https://github.com/domenic/sinon-chai
var sinonChai = require('sinon-chai');
global.chai.use(sinonChai);
