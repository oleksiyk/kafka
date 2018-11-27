'use strict';

/* global step */

var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } } // eslint-disable-line
        function rejected(value) { try { step(generator['throw'](value)); } catch (e) { reject(e); } } // eslint-disable-line
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); } // eslint-disable-line
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, '__esModule', { value: true });
const string_decoder_1 = require('string_decoder'); // eslint-disable-line
/**
 * Pull docker image and wait for it if you need
 * @param dockerode - dockerode
 * @param imageName - name of image to pull
 * @param onProgress - on progress hook
 * @returns Dockerode.Image
 */
exports.pullImageAsync = function (dockerode, imageName, onProgress) {
    return new Promise((resolve, reject) => __awaiter(this, void 0, void 0, function* () { // eslint-disable-line
        const imageNameWithTag = (imageName.indexOf(':') > 0)
            ? imageName
            : `${imageName}:latest`;
        if (yield exports.imageExists(dockerode, imageNameWithTag)) {
            return dockerode.getImage(imageNameWithTag);
        }
        dockerode.pull(imageNameWithTag, (pullError, stream) => {
            if (pullError) {
                reject(pullError);
            }
            if (!stream) {
                throw new Error(`Image '${imageNameWithTag}' doesn't exists`);
            }
            dockerode.modem.followProgress(stream, (error) => {
                // onFinished
                if (error) {
                    reject(error);
                }
                resolve(dockerode.getImage(imageNameWithTag));
            }, onProgress);
        });
    }));
};
/**
 * Execute command inside a container and get output from it, if you need
 * @param container - dockerode.container
 * @param cmd - command to execute
 * @returns result
 */
exports.containerExec = function (container, cmd) {
    // TODO: add detach (don't use stream nor wait for output) options. Useful for daemons
    return new Promise((resolve, error) => {
        container.exec({ Cmd: cmd, AttachStdout: true, AttachStderr: true }, (cErr, exec) => {
            if (cErr) {
                error(cErr);
            }
            exec.start({ hijack: true }, (sErr, stream) => {
                const output = [];
                const decoder = new string_decoder_1.StringDecoder('utf8');
                if (sErr) {
                    error(sErr);
                }
                stream.on('data', (chunk) => {
                    if (chunk) {
                        output.push(decoder.write(chunk));
                    }
                });
                stream.on('end', () => {
                    resolve(output);
                });
            });
        });
    });
};
/**
 * Will wait until container produces specific stdout(console) log.
 * @desc If you're creating and then starting a container, sometimes, you need to wait until a service
 * is fully functional (eg. you can get response from container's exposed port) so you can continue executing code
 * @param container - container
 * @param timeout - how much time (in ms) to wait until it'll throw a error
 */
exports.waitForOutput = function (_container, predicate) {
    var timeout = 30000;
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            reject(`waiting for container excited timeout ${timeout} (default 30s)`);
        }, timeout);
        _container.attach({ stream: true, stdout: true, stderr: true }, (err, res) => {
            if (err) {
                reject(err);
            }
            if (res) {
                res.on('readable', () => {
                    var line = res.read();
                    if (line && predicate(line.toString())) {
                        resolve();
                    }
                });
            } else {
                reject('cannot attach \'readable\' event on container\'s stream');
            }
        });
    });
};

/**
 * Is docker image exists ?
 * @param dockerode
 * @param imageNames - names of images
 * @return true if image with imageName exists otherwise false
 */
exports.imageExists = (dockerode, imageNames) => __awaiter(this, void 0, void 0, function* () {
    const imageNamesArr = (typeof imageNames === 'string')
        ? [imageNames]
        : imageNames;
    const images = yield dockerode.listImages({ filters: { reference: imageNamesArr } });
    return (images.length > 0);
});
//# sourceMappingURL=Index.js.map
