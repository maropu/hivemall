/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.mix.hivemall.mix.solvers;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class Adam implements Solver {
    private static final Log logger = LogFactory.getLog(Adam.class);

    // AdaGrad variable parameters
    private static final float learningRate = 0.001f;
    private static final float beta = 0.9f;
    private static final float gamma = 0.999f;
    private static final float eps_hat = 1e-8f;

    // XXX
    private static final int iter = 1;
    private static final float weightDecay = 0.004f;
    private static final float normalize = (float) 1.0 / iter;

    private final ConcurrentMap<Object, PartialWeight> mapWeights =
            new ConcurrentHashMap<Object, PartialWeight>();

    private class PartialWeight {
        float value = 0.0f;
        float val_m = 0.0f;
        float val_v = 0.0f;
        int ncalls = 0;
    }

    @Override
    public float update(Object feature, float weight, float diff) {
        PartialWeight w = mapWeights.get(feature);
        if (w == null) {
            w = new PartialWeight();
            w.value = weight;
            mapWeights.put(feature, w);
        } else if (Math.abs(w.value - weight) > 0.001f) {
            // TODO: Check ratios to drop diff in this condition
            logger.warn("Drop gradient (global weight=" + w.value + ", local weight=" + weight + ")");
            return w.value;
        }

        // Apply normalization & L2 regularization
        diff *= normalize;
        diff += weightDecay * w.value;

        // Apply adam optimizer
        w.ncalls++;
        double temp = 0.0f;
        final double corr = Math.sqrt(1.0 - Math.pow(gamma, w.ncalls)) / (1.0 - Math.pow(beta, w.ncalls));
        // Update m_{t} <- \beta m_{t-1} + (1 - \beta) g_t
        w.val_m = (float) ((1.0 - beta) * diff + beta * w.val_m);
        // Update v_{t} <- \gamma v_{t-1} + (1 - \gamma) g_t^2
        w.val_v = (float) ((1.0 - gamma) * Math.pow(diff, 2.0) + gamma * w.val_v);
        // Set updates
        temp = w.val_m / (Math.pow(w.val_v, 0.5) + eps_hat);
        temp *= learningRate * corr;
        w.value -= temp;
        return w.value;
    }
}
