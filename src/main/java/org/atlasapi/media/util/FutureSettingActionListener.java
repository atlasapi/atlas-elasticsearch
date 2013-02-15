package org.atlasapi.media.util;

import org.elasticsearch.action.ActionListener;

import com.google.common.util.concurrent.SettableFuture;

public final class FutureSettingActionListener<T> implements ActionListener<T> {

    public static <T> ActionListener<T> setting(SettableFuture<T> future) {
        return new FutureSettingActionListener<T>(future);
    }

    private final SettableFuture<T> result;

    private FutureSettingActionListener(SettableFuture<T> result) {
        this.result = result;
    }

    @Override
    public void onResponse(T input) {
        this.result.set(input);
    }

    @Override
    public void onFailure(Throwable e) {
        this.result.setException(e);
    }
}